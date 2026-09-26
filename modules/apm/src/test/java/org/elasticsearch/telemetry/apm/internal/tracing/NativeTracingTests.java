/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.TransportResponseHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Native and legacy instrumentation share one SDK and preserve context across asynchronous boundaries. */
public class NativeTracingTests extends ESTestCase {
    public void testDisabledLegacyChildDoesNotEndItsOuterSpan() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            fixture.tracer.startTrace("outer", Map.of());
            fixture.tracer.setEnabled(false);
            fixture.tracer.startTrace("disabled-child", Map.of());
            fixture.tracer.stopTrace();
            assertTrue(Span.current().isRecording());
            fixture.tracer.stopTrace();
            assertFalse(Span.current().getSpanContext().isValid());
            assertEquals(List.of("outer"), fixture.exporter.getFinishedSpanItems().stream().map(SpanData::getName).toList());
        }
    }

    public void testDetachedWorkDoesNotInheritNativeParent() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            Span parent = fixture.sdk.getTracer("probe").spanBuilder("parent").startSpan();
            try (var scope = TracingContext.activate(fixture.context, io.opentelemetry.context.Context.root().with(parent))) {
                for (int variant = 0; variant < 3; variant++) {
                    try (var detached = switch (variant) {
                        case 0 -> fixture.context.newEmptyContext();
                        case 1 -> fixture.context.newEmptySystemContext();
                        case 2 -> fixture.context.clearTraceContext();
                        default -> throw new AssertionError(variant);
                    }) {
                        assertFalse(Span.current().getSpanContext().isValid());
                        assertFalse(Span.fromContext(TracingContext.current(fixture.context)).getSpanContext().isValid());
                        assertNull(fixture.context.getHeader(Task.TRACE_ID));
                    }
                    assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                }
            } finally {
                parent.end();
            }
        }
    }

    public void testUntracedTaskBorrowsWithoutEndingOrMutatingParent() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            ThreadPool threadPool = new TestThreadPool("borrowed-task");
            try {
                TaskManager manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.tracer);
                manager.setOpenTelemetry(fixture.sdk);
                Span parent = fixture.sdk.getTracer("probe").spanBuilder("parent").startSpan();
                try (var scope = parent.makeCurrent()) {
                    Task message = manager.register("transport", "page-message", new EmptyRequest(), false);
                    try (var activation = manager.withTaskContext(message)) {
                        assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                    }
                    message.recordTraceFailure(new IllegalArgumentException("protocol failure"));
                    manager.unregister(message);
                    assertTrue(parent.isRecording());
                } finally {
                    parent.end();
                }
                assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
                assertEquals(io.opentelemetry.api.trace.StatusCode.UNSET, fixture.span("parent").getStatus().getStatusCode());
            } finally {
                terminate(threadPool);
            }
        }
    }

    public void testConcurrentQueriesAcrossTcpAndDeferredCallbacks() throws Exception {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            ThreadPool threadPool = new TestThreadPool("native-tcp-probe");
            try (
                MockTransportService sender = MockTransportService.createNewService(
                    Settings.builder().put("node.name", "sender").build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                );
                MockTransportService receiver = MockTransportService.createNewService(
                    Settings.builder().put("node.name", "receiver").build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                );
                var worker = Executors.newSingleThreadExecutor()
            ) {
                sender.getTaskManager().setOpenTelemetry(fixture.sdk);
                receiver.getTaskManager().setOpenTelemetry(fixture.sdk);
                sender.start();
                receiver.start();
                sender.acceptIncomingRequests();
                receiver.acceptIncomingRequests();
                var callbacks = new ConcurrentLinkedQueue<Runnable>();
                var arrived = new CountDownLatch(2);
                sender.registerRequestHandler("internal:trace/ack", threadPool.generic(), EmptyRequest::new, (request, channel, task) -> {
                    assertEquals(
                        task.getTraceContext(),
                        org.elasticsearch.telemetry.tracing.TracingContext.current(threadPool.getThreadContext())
                    );
                    channel.sendResponse(ActionResponse.Empty.INSTANCE);
                });
                receiver.registerRequestHandler(
                    "internal:trace/work",
                    threadPool.generic(),
                    EmptyRequest::new,
                    (request, channel, task) -> {
                        String traceId = Span.current().getSpanContext().getTraceId();
                        callbacks.add(threadPool.getThreadContext().preserveContext(() -> {
                            assertEquals(traceId, Span.current().getSpanContext().getTraceId());
                            Span callback = fixture.sdk.getTracer("probe").spanBuilder("deferred-callback").startSpan();
                            try (var scope = callback.makeCurrent()) {
                                receiver.sendRequest(
                                    sender.getLocalNode(),
                                    "internal:trace/ack",
                                    new EmptyRequest(),
                                    TransportResponseHandler.empty(threadPool.generic(), ActionListener.wrap(ignored -> {
                                        assertEquals(traceId, Span.current().getSpanContext().getTraceId());
                                        callback.end();
                                        channel.sendResponse(ActionResponse.Empty.INSTANCE);
                                    }, failure -> {
                                        callback.end();
                                        channel.sendResponse(failure);
                                    }))
                                );
                            }
                        }));
                        arrived.countDown();
                    }
                );
                try (
                    org.elasticsearch.core.Releasable outbound = safeAwait(
                        listener -> sender.connectToNode(receiver.getLocalNode(), listener)
                    );
                    org.elasticsearch.core.Releasable inbound = safeAwait(
                        listener -> receiver.connectToNode(sender.getLocalNode(), listener)
                    )
                ) {
                    List<Span> queries = new ArrayList<>();
                    List<PlainActionFuture<Void>> results = new ArrayList<>();
                    for (String name : List.of("query-a", "query-b")) {
                        Span query = fixture.sdk.getTracer("probe").spanBuilder(name).setNoParent().startSpan();
                        queries.add(query);
                        var result = new PlainActionFuture<Void>();
                        results.add(result);
                        try (var stored = threadPool.getThreadContext().newStoredContext(); var scope = query.makeCurrent()) {
                            threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, name);
                            sender.sendRequest(
                                receiver.getLocalNode(),
                                "internal:trace/work",
                                new EmptyRequest(),
                                TransportResponseHandler.empty(threadPool.generic(), result)
                            );
                        }
                    }
                    assertTrue(arrived.await(10, TimeUnit.SECONDS));
                    for (Runnable callback : callbacks) {
                        worker.submit(callback).get(10, TimeUnit.SECONDS);
                    }
                    for (PlainActionFuture<Void> result : results) {
                        result.get(10, TimeUnit.SECONDS);
                    }
                    queries.forEach(Span::end);
                    assertBusy(() -> assertEquals(8, fixture.exporter.getFinishedSpanItems().size()));
                    for (Span query : queries) {
                        var trace = fixture.exporter.getFinishedSpanItems()
                            .stream()
                            .filter(span -> span.getTraceId().equals(query.getSpanContext().getTraceId()))
                            .toList();
                        assertEquals(4, trace.size());
                        SpanData work = trace.stream()
                            .filter(span -> span.getName().equals("internal:trace/work"))
                            .findFirst()
                            .orElseThrow();
                        SpanData callback = trace.stream()
                            .filter(span -> span.getName().equals("deferred-callback"))
                            .findFirst()
                            .orElseThrow();
                        SpanData ack = trace.stream().filter(span -> span.getName().equals("internal:trace/ack")).findFirst().orElseThrow();
                        assertEquals(query.getSpanContext().getSpanId(), work.getParentSpanId());
                        assertEquals(work.getSpanId(), callback.getParentSpanId());
                        assertEquals(callback.getSpanId(), ack.getParentSpanId());
                        String opaqueId = trace.stream()
                            .filter(span -> span.getName().startsWith("query-"))
                            .findFirst()
                            .orElseThrow()
                            .getName();
                        assertEquals(opaqueId, work.getAttributes().get(AttributeKey.stringKey("es.x-opaque-id")));
                        assertEquals(opaqueId, ack.getAttributes().get(AttributeKey.stringKey("es.x-opaque-id")));
                    }
                    assertBusy(() -> assertTrue(sender.getTaskManager().getTasks().isEmpty()));
                    assertBusy(() -> assertTrue(receiver.getTaskManager().getTasks().isEmpty()));
                    worker.submit(() -> assertFalse(Span.current().getSpanContext().isValid())).get(10, TimeUnit.SECONDS);
                }
            } finally {
                terminate(threadPool);
            }
        }
    }

    public void testNativeTaskOwnershipAndLocalInstrumentation() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            ThreadPool threadPool = new TestThreadPool("native-task-probe");
            try {
                ThreadContext context = threadPool.getThreadContext();
                context.putHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-11111111111111111111111111111111-2222222222222222-01");
                String opaqueId = randomBoolean() ? randomAlphaOfLength(12) : null;
                if (opaqueId != null) {
                    context.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
                }
                TaskManager manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.tracer);
                manager.setOpenTelemetry(fixture.sdk);
                Task task;
                try (var incoming = context.newTraceContext()) {
                    task = manager.register("transport", "native-task", new EmptyRequest());
                    try (var activation = manager.withTaskContext(task)) {
                        Span local = fixture.sdk.getTracer("probe").spanBuilder("native-child").startSpan();
                        try (
                            var scope = TracingContext.activate(
                                context,
                                TracingContext.withSpan(io.opentelemetry.context.Context.current(), local)
                            )
                        ) {
                            try (var nested = context.newTraceContext()) {
                                Task child = manager.register("transport", "native-action", new EmptyRequest());
                                manager.unregister(child);
                            }
                        } finally {
                            local.end();
                        }
                        assertTrue(Span.current().isRecording());
                    }
                    assertFalse(Span.current().getSpanContext().isValid());
                    manager.unregister(task);
                    manager.unregister(task);
                }
                assertEquals(3, fixture.exporter.getFinishedSpanItems().size());
                assertEquals("2222222222222222", fixture.span("native-task").getParentSpanId());
                assertEquals(fixture.span("native-task").getSpanId(), fixture.span("native-child").getParentSpanId());
                assertEquals(fixture.span("native-child").getSpanId(), fixture.span("native-action").getParentSpanId());
                assertEquals(opaqueId, fixture.span("native-task").getAttributes().get(AttributeKey.stringKey("es.x-opaque-id")));
                assertEquals(opaqueId, fixture.span("native-action").getAttributes().get(AttributeKey.stringKey("es.x-opaque-id")));
                assertTrue(fixture.tracer.getSpans().isEmpty());
            } finally {
                terminate(threadPool);
            }
        }
    }

    public void testLegacyContextOnlyPairOwnsItsChild() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            Traceable parent = () -> "parent";
            fixture.tracer.startTrace(fixture.context, parent, "parent", Map.of());
            try (var scope = fixture.tracer.withScope(parent)) {
                String parentId = Span.current().getSpanContext().getSpanId();
                fixture.tracer.startTrace("unowned-child", Map.of());
                assertNotEquals(parentId, Span.current().getSpanContext().getSpanId());
                fixture.tracer.stopTrace();
                assertTrue(Span.current().isRecording());
                assertEquals(parentId, Span.current().getSpanContext().getSpanId());
            }
            fixture.tracer.stopTrace(parent);
            assertEquals(
                List.of("unowned-child", "parent"),
                fixture.exporter.getFinishedSpanItems().stream().map(SpanData::getName).toList()
            );
        }
    }

    public void testBorrowedScopeDoesNotOwnCompletionAndNativeParentIsAuthoritative() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            Traceable parent = () -> "parent";
            Traceable action = () -> "action";
            fixture.tracer.startTrace(fixture.context, parent, "parent", Map.of());
            String parentId;
            try (var scope = fixture.tracer.withScope(parent)) {
                parentId = Span.current().getSpanContext().getSpanId();
                Span local = fixture.sdk.getTracer("probe").spanBuilder("otel-child").startSpan();
                try (var localScope = local.makeCurrent(); var childContext = fixture.context.newTraceContext()) {
                    fixture.tracer.startTrace(fixture.context, action, "action", Map.of());
                    fixture.tracer.stopTrace(action);
                } finally {
                    local.end();
                }
                assertTrue(Span.current().isRecording());
            }
            assertFalse(Span.current().getSpanContext().isValid());
            fixture.tracer.stopTrace(parent);
            assertEquals(fixture.span("otel-child").getSpanId(), fixture.span("action").getParentSpanId());
            assertEquals(parentId, fixture.span("otel-child").getParentSpanId());
        }
    }

    public void testUnsampledTraceRetainsForwardingHeaders() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOff(), 10)) {
            fixture.context.putHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-11111111111111111111111111111111-2222222222222222-00");
            try (var incoming = fixture.context.newTraceContext()) {
                fixture.tracer.startTrace(fixture.context, () -> "entry", "entry", Map.of());
                assertNotNull(fixture.context.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
                assertNotNull(fixture.context.getTransient(Task.APM_TRACE_CONTEXT));
                try (var next = fixture.context.newTraceContext()) {
                    assertNotNull(fixture.context.getTransient(Task.PARENT_TRACE_PARENT_HEADER));
                }
            }
        }
    }

    public void testFilteredAndDepthLimitedChildRetainForwardingContext() {
        for (boolean filter : List.of(false, true)) {
            Settings settings = filter ? Settings.builder().putList("telemetry.tracing.names.exclude", "child").build() : Settings.EMPTY;
            try (Fixture fixture = new Fixture(settings, Sampler.alwaysOn(), filter ? 10 : 0)) {
                Traceable parent = () -> "parent";
                fixture.tracer.startTrace(fixture.context, parent, "parent", Map.of());
                assertNotNull(fixture.context.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
                try (var childContext = fixture.context.newTraceContext()) {
                    fixture.tracer.startTrace(fixture.context, () -> "child", "child", Map.of());
                    assertNotNull(fixture.context.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
                    assertNotNull(fixture.context.getTransient(Task.APM_TRACE_CONTEXT));
                    try (var grandchildContext = fixture.context.newTraceContext()) {
                        assertTrue(fixture.context.hasParentApmTraceContext());
                    }
                }
                fixture.tracer.stopTrace(parent);
            }
        }
    }

    public void testDirectFailureRestoresPreservedContext() {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        try (var original = context.newStoredContext()) {
            AtomicReference<String> observed = new AtomicReference<>();
            AbstractRunnable wrapped;
            try (var queryContext = context.newStoredContext()) {
                context.putHeader("query", "query-a");
                wrapped = (AbstractRunnable) context.preserveContext(new AbstractRunnable() {
                    @Override
                    protected void doRun() {}

                    @Override
                    public void onFailure(Exception failure) {
                        observed.set(context.getHeader("query"));
                    }
                });
            }
            context.putHeader("query", "query-b");
            wrapped.onFailure(new IllegalStateException("rejected"));
            assertEquals("query-a", observed.get());
            assertEquals("query-b", context.getHeader("query"));
        }
    }

    public void testRemoteTraceParentStartsTaskSpan() {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            ThreadPool threadPool = new TestThreadPool("trace-probe");
            try {
                ThreadContext context = threadPool.getThreadContext();
                context.putHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-11111111111111111111111111111111-2222222222222222-01");
                TaskManager manager = new TaskManager(Settings.EMPTY, threadPool, Set.of(), fixture.tracer);
                try (var incoming = context.newTraceContext()) {
                    assertNotNull(context.getTransient(Task.PARENT_TRACE_PARENT_HEADER));
                    assertFalse(context.hasParentApmTraceContext());
                    Task task = manager.register("transport", "indices:data/read/esql/data", new EmptyRequest());
                    manager.unregister(task);
                    assertEquals(1, fixture.exporter.getFinishedSpanItems().size());
                    assertEquals("2222222222222222", fixture.span("indices:data/read/esql/data").getParentSpanId());
                    assertTrue(fixture.tracer.getSpans().isEmpty());
                }
            } finally {
                terminate(threadPool);
            }
        }
    }

    public void testTwoLiveQueriesReuseWorkerAndCompleteAcrossThreads() throws Exception {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10); var worker = Executors.newSingleThreadExecutor()) {
            List<Runnable> slices = new ArrayList<>();
            List<Traceable> queries = new ArrayList<>();
            for (String queryId : List.of("query-a", "query-b")) {
                Traceable query = () -> queryId;
                queries.add(query);
                try (var isolated = fixture.context.newStoredContext()) {
                    fixture.context.putHeader("query", queryId);
                    fixture.tracer.startTrace(fixture.context, query, queryId, Map.of());
                    slices.add(fixture.context.preserveContext(() -> {
                        assertEquals(queryId, fixture.context.getHeader("query"));
                        assertTrue(Span.current().getSpanContext().isValid());
                        try (var scope = fixture.tracer.withScope(query)) {
                            Span local = fixture.sdk.getTracer("probe").spanBuilder(queryId + "-local").startSpan();
                            local.end();
                            try (var child = fixture.context.newTraceContext()) {
                                Traceable action = () -> queryId + "-action";
                                fixture.tracer.startTrace(fixture.context, action, queryId + "-action", Map.of());
                                fixture.tracer.stopTrace(action);
                            }
                        }
                        assertTrue(Span.current().getSpanContext().isValid());
                    }));
                }
            }
            for (int iteration = 0; iteration < 3; iteration++) {
                for (Runnable slice : slices) {
                    worker.submit(slice).get(10, TimeUnit.SECONDS);
                }
            }
            worker.submit(() -> {
                assertNull(fixture.context.getHeader("query"));
                assertFalse(Span.current().getSpanContext().isValid());
                queries.forEach(fixture.tracer::stopTrace);
            }).get(10, TimeUnit.SECONDS);
            for (String queryId : List.of("query-a", "query-b")) {
                SpanData parent = fixture.span(queryId);
                for (SpanData span : fixture.exporter.getFinishedSpanItems()) {
                    if (span.getName().startsWith(queryId + "-")) {
                        assertEquals(parent.getTraceId(), span.getTraceId());
                        assertEquals(parent.getSpanId(), span.getParentSpanId());
                    }
                }
            }
            assertTrue(fixture.tracer.getSpans().isEmpty());
        }
    }

    public void testSynchronousTransportHandlerFailureIsRecordedBeforeCompletion() throws Exception {
        try (Fixture fixture = new Fixture(Settings.EMPTY, Sampler.alwaysOn(), 10)) {
            ThreadPool threadPool = new TestThreadPool("throwing-handler");
            try (
                var sender = MockTransportService.createNewService(
                    Settings.builder().put("node.name", "sender").build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                );
                var receiver = MockTransportService.createNewService(
                    Settings.builder().put("node.name", "receiver").build(),
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool
                )
            ) {
                sender.getTaskManager().setOpenTelemetry(fixture.sdk);
                receiver.getTaskManager().setOpenTelemetry(fixture.sdk);
                sender.start();
                receiver.start();
                sender.acceptIncomingRequests();
                receiver.acceptIncomingRequests();
                String action = "internal:trace/throw";
                receiver.registerRequestHandler(action, threadPool.generic(), EmptyRequest::new, (request, channel, task) -> {
                    throw new IllegalStateException("handler failed before sending a response");
                });
                try (
                    org.elasticsearch.core.Releasable connection = safeAwait(
                        listener -> sender.connectToNode(receiver.getLocalNode(), listener)
                    )
                ) {
                    Span root = fixture.sdk.getTracer("test").spanBuilder("request").startSpan();
                    try {
                        var result = new PlainActionFuture<Void>();
                        try (var scope = root.makeCurrent()) {
                            sender.sendRequest(
                                receiver.getLocalNode(),
                                action,
                                new EmptyRequest(),
                                TransportResponseHandler.empty(threadPool.generic(), result)
                            );
                        }
                        var failure = expectThrows(java.util.concurrent.ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
                        assertEquals(
                            "handler failed before sending a response",
                            org.elasticsearch.ExceptionsHelper.unwrapCause(failure.getCause()).getMessage()
                        );
                        assertBusy(() -> {
                            SpanData recorded = fixture.span(action);
                            assertEquals(io.opentelemetry.api.trace.StatusCode.ERROR, recorded.getStatus().getStatusCode());
                            assertEquals(
                                "failure",
                                recorded.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("es.outcome"))
                            );
                            assertEquals(
                                IllegalStateException.class.getName(),
                                recorded.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("error.type"))
                            );
                            assertEquals(root.getSpanContext().getSpanId(), recorded.getParentSpanId());
                            assertTrue(receiver.getTaskManager().getTasks().isEmpty());
                        });
                    } finally {
                        root.end();
                    }
                }
            } finally {
                terminate(threadPool);
            }
        }
    }

    private static class Fixture implements AutoCloseable {
        final InMemorySpanExporter exporter = InMemorySpanExporter.create();
        final OpenTelemetrySdk sdk;
        final APMTracer tracer;
        final ThreadContext context = new ThreadContext(Settings.EMPTY);

        Fixture(Settings extraSettings, Sampler sampler, int depth) {
            SdkTracerProvider provider = SdkTracerProvider.builder()
                .setSampler(sampler)
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
            sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
            Settings settings = Settings.builder().put("telemetry.tracing.enabled", true).put(extraSettings).build();
            tracer = new APMTracer(settings, () -> sdk, depth, false);
            tracer.setNodeName("probe-node");
            tracer.setClusterName("probe-cluster");
            tracer.start();
        }

        SpanData span(String name) {
            return exporter.getFinishedSpanItems().stream().filter(span -> span.getName().equals(name)).findFirst().orElseThrow();
        }

        @Override
        public void close() {
            tracer.close();
            sdk.close();
        }
    }
}

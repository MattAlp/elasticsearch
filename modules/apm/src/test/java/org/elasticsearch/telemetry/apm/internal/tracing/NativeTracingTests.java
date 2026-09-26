/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Native and legacy instrumentation share one SDK and preserve context across asynchronous boundaries. */
public class NativeTracingTests extends ESTestCase {
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

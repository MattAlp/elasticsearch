/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

/** Checks phase ownership independently of planning so failure and callback paths are deterministic. */
public class EsqlTracingTests extends ESTestCase {
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
        .build();
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final EsqlTracing tracing = new EsqlTracing(sdk, threadContext);

    @After
    public void closeSdk() {
        sdk.close();
    }

    public void testConcurrentPhasesResumeUnderTheirOwnContexts() throws Exception {
        try (var worker = Executors.newSingleThreadExecutor()) {
            List<Span> roots = new ArrayList<>();
            List<ActionListener<Void>> callbacks = new ArrayList<>();
            for (String name : List.of("first", "second")) {
                Span root = sdk.getTracer("test").spanBuilder(name).setNoParent().startSpan();
                roots.add(root);
                try (var scope = root.makeCurrent()) {
                    tracing.phase(
                        "execution",
                        ActionListener.<Void>wrap(
                            ignored -> { assertEquals(root.getSpanContext(), Span.current().getSpanContext()); },
                            failure -> fail(failure)
                        ),
                        callbacks::add
                    );
                }
            }
            assertTrue(exporter.getFinishedSpanItems().isEmpty());
            for (int index = callbacks.size() - 1; index >= 0; index--) {
                ActionListener<Void> callback = callbacks.get(index);
                worker.submit(() -> {
                    callback.onResponse(null);
                    assertFalse(Span.current().getSpanContext().isValid());
                    assertTrue(threadContext.isDefaultContext());
                }).get(10, TimeUnit.SECONDS);
            }
            roots.forEach(Span::end);
            assertEquals(4, exporter.getFinishedSpanItems().size());
            for (Span root : roots) {
                SpanData phase = exporter.getFinishedSpanItems()
                    .stream()
                    .filter(span -> span.getName().equals("esql.execution") && span.getTraceId().equals(root.getSpanContext().getTraceId()))
                    .findFirst()
                    .orElseThrow();
                assertEquals(root.getSpanContext().getSpanId(), phase.getParentSpanId());
            }
        }
    }

    public void testSynchronousFailureEndsPhase() {
        Span root = sdk.getTracer("test").spanBuilder("query").startSpan();
        try (var scope = root.makeCurrent()) {
            expectThrows(
                IllegalArgumentException.class,
                () -> tracing.phase("planning", () -> { throw new IllegalArgumentException("failed"); })
            );
            assertEquals(root.getSpanContext(), Span.current().getSpanContext());
            SpanData phase = exporter.getFinishedSpanItems().getFirst();
            assertEquals(StatusCode.ERROR, phase.getStatus().getStatusCode());
            assertEquals("failure", phase.getAttributes().get(stringKey("es.outcome")));
        } finally {
            root.end();
        }
    }

    public void testCancellationIsAnOutcomeNotServerError() {
        Span root = sdk.getTracer("test").spanBuilder("query").startSpan();
        AtomicReference<Exception> observed = new AtomicReference<>();
        try (var scope = root.makeCurrent()) {
            tracing.phase("execution", ActionListener.<Void>wrap(ignored -> fail("unexpected success"), observed::set), listener -> {
                listener.onFailure(new TaskCancelledException("cancelled"));
            });
            assertNotNull(observed.get());
            SpanData phase = exporter.getFinishedSpanItems().getFirst();
            assertEquals(StatusCode.UNSET, phase.getStatus().getStatusCode());
            assertEquals("cancelled", phase.getAttributes().get(stringKey("es.outcome")));
        } finally {
            root.end();
        }
    }

    public void testUntracedWorkDoesNotCreateRootSpans() {
        assertEquals(42, tracing.phase("planning", () -> 42).intValue());
        assertTrue(exporter.getFinishedSpanItems().isEmpty());
    }
}

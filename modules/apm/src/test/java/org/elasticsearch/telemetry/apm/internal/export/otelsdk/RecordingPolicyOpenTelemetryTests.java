/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/** Local capture controls must not change distributed sampling or acquire ownership of a suppressed span's parent. */
public class RecordingPolicyOpenTelemetryTests extends ESTestCase {
    public void testDepthZeroStillRecordsTheNextNodeEntry() {
        BiPredicate<Context, String> rootsOnly = (parent, name) -> TracingContext.childDepth(parent) == 0;
        try (
            var coordinator = new Fixture(rootsOnly, new ElasticTracestateSampler(1));
            var remote = new Fixture(rootsOnly, new ElasticTracestateSampler(1))
        ) {
            Span root = coordinator.api.getTracer("test").spanBuilder("request").setNoParent().startSpan();
            Context rootContext = TracingContext.withSpan(Context.root(), root);
            Span suppressed = coordinator.api.getTracer("test").spanBuilder("coordinator-task").setParent(rootContext).startSpan();
            Context forwarding = TracingContext.withSpan(rootContext, suppressed);
            assertFalse(suppressed.isRecording());
            assertEquals(root.getSpanContext(), suppressed.getSpanContext());
            assertEquals(Integer.valueOf(0), forwarding.get(TracingContext.LOCAL_DEPTH));
            suppressed.end();
            assertTrue(root.isRecording());

            ThreadContext wire = new ThreadContext(Settings.EMPTY);
            wire.putHeader(TracingContext.headers(forwarding, Map.of()));
            Context extracted = TracingContext.extract(wire);
            assertTrue(Span.fromContext(extracted).getSpanContext().isSampled());
            assertEquals(root.getSpanContext().getTraceFlags(), Span.fromContext(extracted).getSpanContext().getTraceFlags());
            Span entry = remote.api.getTracer("test").spanBuilder("remote-task").setParent(extracted).startSpan();
            assertTrue(entry.isRecording());
            entry.end();
            root.end();
            assertEquals(1, coordinator.exporter.getFinishedSpanItems().size());
            assertEquals(root.getSpanContext().getTraceId(), remote.onlySpan().getTraceId());
            assertEquals(root.getSpanContext().getSpanId(), remote.onlySpan().getParentSpanId());
        }
    }

    public void testExcludedSpanDoesNotHideAllowedDescendant() {
        try (
            var fixture = new Fixture(
                (parent, name) -> name.equals("excluded") == false && TracingContext.childDepth(parent) <= 1,
                new ElasticTracestateSampler(1)
            )
        ) {
            Span root = fixture.api.getTracer("test").spanBuilder("root").startSpan();
            Context rootContext = TracingContext.withSpan(Context.root(), root);
            Span excluded = fixture.api.getTracer("test").spanBuilder("excluded").setParent(rootContext).startSpan();
            try (var scope = TracingContext.withSpan(rootContext, excluded).makeCurrent()) {
                Span allowed = fixture.api.getTracer("test").spanBuilder("allowed").startSpan();
                assertTrue(allowed.isRecording());
                allowed.end();
            }
            excluded.end();
            assertTrue(root.isRecording());
            root.end();
            assertEquals(2, fixture.exporter.getFinishedSpanItems().size());
            SpanData allowed = fixture.exporter.getFinishedSpanItems().getFirst();
            assertEquals("allowed", allowed.getName());
            assertEquals(root.getSpanContext().getSpanId(), allowed.getParentSpanId());
        }
    }

    public void testActualSamplingDecisionStillPropagates() {
        try (
            var local = new Fixture((parent, name) -> true, new ElasticTracestateSampler(0));
            var remote = new Fixture((parent, name) -> true, new ElasticTracestateSampler(1))
        ) {
            Span root = local.api.getTracer("test").spanBuilder("unsampled").setNoParent().startSpan();
            assertFalse(root.getSpanContext().isSampled());
            ThreadContext wire = new ThreadContext(Settings.EMPTY);
            wire.putHeader(TracingContext.headers(TracingContext.withSpan(Context.root(), root), Map.of()));
            Context extracted = TracingContext.extract(wire);
            assertEquals(root.getSpanContext().getTraceFlags(), Span.fromContext(extracted).getSpanContext().getTraceFlags());
            Span child = remote.api.getTracer("test").spanBuilder("remote").setParent(extracted).startSpan();
            assertFalse(child.isRecording());
            assertEquals(root.getSpanContext().getTraceId(), child.getSpanContext().getTraceId());
            child.end();
            root.end();
            assertTrue(local.exporter.getFinishedSpanItems().isEmpty());
            assertTrue(remote.exporter.getFinishedSpanItems().isEmpty());
        }
    }

    public void testNoParentDoesNotBorrowAmbientSpan() {
        try (var fixture = new Fixture((parent, name) -> name.equals("root"), new ElasticTracestateSampler(1))) {
            Span root = fixture.api.getTracer("test").spanBuilder("root").startSpan();
            try (var scope = root.makeCurrent()) {
                Span excluded = fixture.api.getTracer("test").spanBuilder("excluded").setNoParent().startSpan();
                assertFalse(excluded.getSpanContext().isValid());
                excluded.end();
                assertTrue(root.isRecording());
            } finally {
                root.end();
            }
        }
    }

    public void testAllowedBuilderPreservesScopeAndSpanMetadata() {
        try (var fixture = new Fixture((parent, name) -> true, new ElasticTracestateSampler(1))) {
            Span link = fixture.sdk.getTracer("link").spanBuilder("link").startSpan();
            Span span = fixture.api.tracerBuilder("scope")
                .setInstrumentationVersion("version")
                .setSchemaUrl("https://example.test/schema")
                .build()
                .spanBuilder("delegated")
                .setNoParent()
                .setSpanKind(SpanKind.SERVER)
                .addLink(link.getSpanContext())
                .addLink(link.getSpanContext(), Attributes.of(AttributeKey.stringKey("link"), "value"))
                .setAttribute("string", "value")
                .setAttribute("long", 3L)
                .setAttribute("double", 2.5)
                .setAttribute("boolean", true)
                .setAllAttributes(Attributes.of(AttributeKey.stringKey("extra"), "attribute"))
                .setStartTimestamp(1L, TimeUnit.SECONDS)
                .startSpan();
            span.end();
            SpanData recorded = fixture.onlySpan();
            assertEquals("scope", recorded.getInstrumentationScopeInfo().getName());
            assertEquals("version", recorded.getInstrumentationScopeInfo().getVersion());
            assertEquals("https://example.test/schema", recorded.getInstrumentationScopeInfo().getSchemaUrl());
            assertEquals(SpanKind.SERVER, recorded.getKind());
            assertEquals(2, recorded.getLinks().size());
            assertEquals(5, recorded.getAttributes().size());
            assertEquals(TimeUnit.SECONDS.toNanos(1), recorded.getStartEpochNanos());
            link.end();
        }
    }

    private static class Fixture implements AutoCloseable {
        private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
        private final OpenTelemetrySdk sdk;
        private final OpenTelemetry api;

        Fixture(BiPredicate<Context, String> filter, Sampler sampler) {
            sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(
                    SdkTracerProvider.builder().setSampler(sampler).addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
                )
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
            api = new RecordingPolicyOpenTelemetry(sdk, filter);
        }

        SpanData onlySpan() {
            assertEquals(1, exporter.getFinishedSpanItems().size());
            return exporter.getFinishedSpanItems().getFirst();
        }

        @Override
        public void close() {
            sdk.close();
        }
    }
}

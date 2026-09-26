/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportTracerSupplier;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

/** Uses exported native spans to check HTTP metadata, ownership and legacy redaction policy together. */
public class APMHttpServerInstrumentationTests extends ESTestCase {
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
        .build();
    private final APMTracer tracer = new APMTracer(Settings.builder().put("telemetry.tracing.enabled", true).build(), MeterProvider::noop) {
        @Override
        public OpenTelemetry getOpenTelemetry() {
            return sdk;
        }
    };
    private final APMHttpServerInstrumentation instrumentation = new APMHttpServerInstrumentation(tracer);

    @Before
    public void initializeMetadata() {
        tracer.setNodeName("node");
        tracer.setClusterName("cluster");
    }

    @After
    public void closeTelemetry() {
        sdk.close();
        tracer.close();
    }

    public void testRequestAttributesAndRedaction() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withScheme("https")
            .withPath("/my-index/_search")
            .withHeaders(Map.of("Accept-Encoding", List.of("gzip"), "Authorization", List.of("secret")))
            .build();
        instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/{index}/_search");
        instrumentation.end(request, response(RestStatus.OK));
        SpanData span = exported();
        assertEquals("GET /{index}/_search", span.getName());
        assertEquals("GET", span.getAttributes().get(stringKey("http.method")));
        assertEquals("GET", span.getAttributes().get(stringKey("http.request.method")));
        assertEquals("https", span.getAttributes().get(stringKey("url.scheme")));
        assertEquals("/{index}/_search", span.getAttributes().get(stringKey("http.route")));
        assertEquals("gzip", span.getAttributes().get(stringKey("http.request.headers.accept_encoding")));
        assertEquals("[REDACTED]", span.getAttributes().get(stringKey("http.request.headers.authorization")));
        assertEquals(Long.valueOf(200), span.getAttributes().get(longKey("http.response.status_code")));
    }

    public void testRepeatedRequestsWithoutExportEndpointDoNotWarn() {
        try (var unconfigured = new APMTracer(Settings.builder().put("telemetry.tracing.enabled", true).build(), MeterProvider::noop)) {
            unconfigured.setNodeName("node");
            unconfigured.setClusterName("cluster");
            assertSame(OpenTelemetry.noop(), unconfigured.getOpenTelemetry());
            var http = new APMHttpServerInstrumentation(unconfigured);
            var context = new ThreadContext(Settings.EMPTY);
            try (var log = MockLog.capture(OtelSdkExportTracerSupplier.class)) {
                log.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "HTTP requests reuse the initialized no-op provider",
                        OtelSdkExportTracerSupplier.class.getName(),
                        Level.WARN,
                        "*trace export is disabled*"
                    )
                );
                for (int attempt = 0; attempt < 10; attempt++) {
                    var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET).withPath("/").build();
                    http.start(context, request, "/");
                    http.end(request, response(RestStatus.OK));
                }
                log.assertAllExpectationsMatched();
            }
        }
    }

    public void testForwardedAttributes() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withScheme("http")
            .withPath("/my-index/_search?from=0")
            .withHeaders(Map.of("Forwarded", List.of("for=1.1.1.1;proto=https"), "Host", List.of("elastic.co:443")))
            .build();
        instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/{index}/_search");
        instrumentation.end(request, response(RestStatus.OK));
        SpanData span = exported();
        assertEquals("https", span.getAttributes().get(stringKey("url.scheme")));
        assertEquals("from=0", span.getAttributes().get(stringKey("url.query")));
        assertEquals("elastic.co", span.getAttributes().get(stringKey("server.address")));
        assertEquals(Long.valueOf(443), span.getAttributes().get(longKey("server.port")));
        assertEquals("1.1.1.1", span.getAttributes().get(stringKey("client.address")));
    }

    public void testExceptionDoesNotEmitStackByDefault() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/");
        instrumentation.recordException(request, new IllegalStateException("failed"));
        instrumentation.end(request, response(RestStatus.INTERNAL_SERVER_ERROR));
        SpanData span = exported();
        assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
        assertEquals(1, span.getEvents().size());
        assertEquals(IllegalStateException.class.getName(), span.getEvents().getFirst().getAttributes().get(stringKey("exception.type")));
        assertNull(span.getEvents().getFirst().getAttributes().get(stringKey("exception.stacktrace")));
    }

    public void testClientErrorIsNotServerFailure() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/");
        instrumentation.end(request, response(RestStatus.BAD_REQUEST));
        assertEquals(StatusCode.UNSET, exported().getStatus().getStatusCode());
    }

    public void testResponseEndsSpanOnlyOnce() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        instrumentation.start(new ThreadContext(Settings.EMPTY), request, "/");
        instrumentation.end(request, response(RestStatus.OK));
        request.finishTrace();
        assertNotNull(exported());
    }

    private SpanData exported() {
        assertEquals(1, exporter.getFinishedSpanItems().size());
        return exporter.getFinishedSpanItems().getFirst();
    }

    private RestResponse response(RestStatus status) {
        return new RestResponse(status, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
    }
}

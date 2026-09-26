/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.TelemetryPlugin;
import org.elasticsearch.telemetry.TelemetryLogResourceProvider;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.AfterClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/** Captures actual ES|QL planning and multi-node driver spans without an external telemetry service. */
@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class EsqlNativeTracingIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), RecordingTelemetryPlugin.class);
    }

    @AfterClass
    public static void clearRecordings() {
        RecordingTelemetryPlugin.instances.clear();
    }

    public void testConcurrentDistributedAggregationTrace() throws Exception {
        assertAcked(
            prepareCreate("traced-data").setSettings(indexSettings(2, 0)).setMapping("category", "type=keyword", "value", "type=long")
        );
        var bulk = client().prepareBulk();
        for (int document = 0; document < 4096; document++) {
            bulk.add(
                client().prepareIndex("traced-data")
                    .setId(Integer.toString(document))
                    .setSource("category", "group-" + document % 4, "value", 1)
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh("traced-data");
        ensureGreen("traced-data");

        var rootTracer = RecordingTelemetryPlugin.instances.getFirst().sdk.getTracer("test.queries");
        List<Span> roots = new ArrayList<>();
        List<ActionFuture<EsqlQueryResponse>> responses = new ArrayList<>();
        try {
            for (String queryName : List.of("query-a", "query-b")) {
                Span root = rootTracer.spanBuilder(queryName).setNoParent().startSpan();
                roots.add(root);
                try (var scope = root.makeCurrent()) {
                    var request = EsqlQueryRequest.syncEsqlQueryRequest(
                        "FROM traced-data | STATS total = SUM(value) BY category | SORT category"
                    ).pragmas(new QueryPragmas(Settings.builder().put("page_size", 1).build()));
                    responses.add(client().execute(EsqlQueryAction.INSTANCE, request));
                }
            }
            for (var responseFuture : responses) {
                try (var response = responseFuture.actionGet()) {
                    assertOk(response);
                    int rows = 0;
                    var values = response.values();
                    while (values.hasNext()) {
                        values.next();
                        rows++;
                    }
                    assertEquals(4, rows);
                }
            }
        } finally {
            roots.forEach(Span::end);
        }

        for (Span root : roots) {
            String traceId = root.getSpanContext().getTraceId();
            assertBusy(() -> {
                List<SpanData> trace = RecordingTelemetryPlugin.instances.stream()
                    .flatMap(plugin -> plugin.exporter.getFinishedSpanItems().stream())
                    .filter(span -> span.getTraceId().equals(traceId))
                    .toList();
                assertTrue(trace.stream().anyMatch(span -> span.getName().equals("esql.planning.analysis")));
                assertTrue(trace.stream().anyMatch(span -> span.getName().equals("esql.execution")));
                assertTrue(trace.stream().anyMatch(span -> span.getName().equals("esql.planning.local")));
                Set<String> spanIds = new HashSet<>();
                trace.forEach(span -> spanIds.add(span.getSpanId()));
                Set<String> driverNodes = new HashSet<>();
                for (SpanData span : trace) {
                    if (span.getSpanId().equals(root.getSpanContext().getSpanId()) == false) {
                        assertTrue("missing parent for " + span.getName(), spanIds.contains(span.getParentSpanId()));
                    }
                    assertFalse(span.getName().equals("internal:data/read/esql/exchange"));
                    if (span.getName().equals("indices:data/read/esql/compute")) {
                        driverNodes.add(span.getAttributes().get(stringKey("es.node.id")));
                        assertNotNull(span.getAttributes().get(stringKey("esql.role")));
                    }
                }
                assertTrue("expected drivers on both nodes: " + driverNodes, driverNodes.size() >= 2);
            });
            RecordingTelemetryPlugin.instances.stream()
                .flatMap(plugin -> plugin.exporter.getFinishedSpanItems().stream())
                .filter(span -> span.getTraceId().equals(traceId))
                .forEach(
                    span -> logger.info(
                        "captured trace [{}] span [{}] parent [{}] name [{}]",
                        traceId,
                        span.getSpanId(),
                        span.getParentSpanId(),
                        span.getName()
                    )
                );
        }
    }

    /** Each node owns a real SDK; only recording fixtures are shared to inspect the distributed result. */
    public static final class RecordingTelemetryPlugin extends Plugin implements TelemetryPlugin {
        static final List<RecordingTelemetryPlugin> instances = new CopyOnWriteArrayList<>();
        final InMemorySpanExporter exporter = InMemorySpanExporter.create();
        final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
            .build();

        public RecordingTelemetryPlugin() {
            instances.add(this);
        }

        @Override
        public TelemetryProvider getTelemetryProvider(
            Environment environment,
            List<TelemetryLoggingFilterProvider> filters,
            TelemetryLogResourceProvider resourceProvider
        ) {
            return new TelemetryProvider() {
                @Override
                public Tracer getTracer() {
                    return Tracer.NOOP;
                }

                @Override
                public OpenTelemetry getOpenTelemetry() {
                    return sdk;
                }

                @Override
                public MeterRegistry getMeterRegistry() {
                    return MeterRegistry.NOOP;
                }

                @Override
                public HttpServerInstrumentation getHttpServerInstrumentation() {
                    return HttpServerInstrumentation.NOOP;
                }

                @Override
                public void attemptFlush() {
                    sdk.getSdkTracerProvider().forceFlush();
                }
            };
        }

        @Override
        public void close() {
            sdk.close();
        }
    }
}

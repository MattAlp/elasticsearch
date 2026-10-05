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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InternalTelemetryVersion;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracingService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * {@link TraceSupplier} that exports spans via OTLP/gRPC using its own {@link SdkTracerProvider}.
 */
public class OtelSdkExportTracerSupplier implements TraceSupplier {

    private static final Logger logger = LogManager.getLogger(OtelSdkExportTracerSupplier.class);

    private final Settings settings;
    private final Supplier<MeterProvider> meterProvider;
    private final Object mutex = new Object();
    private volatile OpenTelemetrySdk openTelemetrySdk;
    private boolean initialized;
    private boolean closed;
    private volatile UnaryOperator<Attributes> attributeSanitizer = UnaryOperator.identity();

    /** Installs privacy enforcement before the SDK exporter is initialized. */
    public void setAttributeSanitizer(UnaryOperator<Attributes> sanitizer) {
        attributeSanitizer = sanitizer;
    }

    public boolean hasEndpoint() {
        return OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.get(settings).isEmpty() == false;
    }

    public OtelSdkExportTracerSupplier(Settings settings, Supplier<MeterProvider> meterProvider) {
        this.settings = settings;
        this.meterProvider = meterProvider;
    }

    @Override
    public OpenTelemetry get() {
        synchronized (mutex) {
            if (closed) {
                return OpenTelemetry.noop();
            }
            if (initialized == false) {
                openTelemetrySdk = createOpenTelemetrySdk();
                initialized = true;
            }
            return openTelemetrySdk == null ? OpenTelemetry.noop() : openTelemetrySdk;
        }
    }

    @Override
    public CompletableResultCode attemptFlushTraces() {
        OpenTelemetrySdk openTelemetrySdk;
        synchronized (mutex) {
            openTelemetrySdk = this.openTelemetrySdk;
        }
        return openTelemetrySdk == null ? CompletableResultCode.ofSuccess() : openTelemetrySdk.getSdkTracerProvider().forceFlush();
    }

    @Override
    public void close() {
        synchronized (mutex) {
            closed = true;
            if (openTelemetrySdk != null) {
                openTelemetrySdk.getSdkTracerProvider().close();
                openTelemetrySdk = null;
            }
            initialized = false;
        }
    }

    private OpenTelemetrySdk createOpenTelemetrySdk() {
        String endpoint = OtelSdkSettings.TELEMETRY_EXPORT_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isEmpty()) {
            logger.warn("[telemetry.export.endpoint] is not configured; trace export is disabled");
            return null;
        }

        TimeValue interval = OtelSdkSettings.TELEMETRY_EXPORT_INTERVAL.get(settings);
        double sampleRate = OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.get(settings);
        int maxQueueSize = OtelSdkSettings.TELEMETRY_TRACING_MAX_QUEUE_SIZE.get(settings);
        int maxExportBatchSize = OtelSdkSettings.TELEMETRY_TRACING_MAX_BATCH_SIZE.get(settings);
        boolean universalProfilingEnabled = OtelSdkSettings.TELEMETRY_TRACING_UNIVERSAL_PROFILING_ENABLED.get(settings);

        // InternalTelemetryVersion is @Internal but is the only way to opt into stable SemConv names in 1.62.0.
        OtlpGrpcSpanExporterBuilder builder = OtlpGrpcSpanExporter.builder()
            .setEndpoint(endpoint)
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setTimeout(OtelSdkSettings.TELEMETRY_EXPORT_SEND_TIMEOUT.get(settings).toDuration())
            .setConnectTimeout(OtelSdkSettings.TELEMETRY_EXPORT_CONNECT_TIMEOUT.get(settings).toDuration())
            .setRetryPolicy(OtelSdkSettings.OTLP_RETRY_POLICY);
        String authHeader = OtelSdkExportMeterSupplier.buildOtlpAuthorizationHeader(settings);
        if (authHeader != null) {
            builder.addHeader("Authorization", authHeader);
        }
        OtelSdkExportMeterSupplier.configureTls(settings, builder::setSslContext);
        SpanExporter exporter = builder.build();
        if (universalProfilingEnabled) {
            exporter = applyProfilerHostId(exporter);
        }

        BatchSpanProcessor processor = BatchSpanProcessor.builder(
            new SanitizingSpanExporter(exporter, attributes -> attributeSanitizer.apply(attributes))
        )
            .setMeterProvider(meterProvider)
            .setInternalTelemetryVersion(InternalTelemetryVersion.LATEST)
            .setScheduleDelay(interval.millis(), TimeUnit.MILLISECONDS)
            .setMaxQueueSize(maxQueueSize)
            .setMaxExportBatchSize(maxExportBatchSize)
            .build();

        // TODO: emit the modern th: tracestate instead of ot=p: once exporting to EDOT gateway
        Sampler sampler = new ElasticTracestateSampler(sampleRate);

        Resource resource = OtelSdkResource.get(settings);
        SpanProcessor traceProcessor = processor;
        if (universalProfilingEnabled) {
            traceProcessor = newUniversalProfilingProcessor(
                processor,
                resource,
                OtelSdkSettings.TELEMETRY_TRACING_UNIVERSAL_PROFILING_SOCKET_DIR.get(settings)
            );
        }
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .setResource(resource)
            .setSampler(sampler)
            .addSpanProcessor(traceProcessor)
            .build();

        return OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }

    private static SpanExporter applyProfilerHostId(SpanExporter exporter) {
        try {
            Class<?> exporterClass = loadUniversalProfilingClass("co.elastic.otel.hostid.ProfilerHostIdApplyingSpanExporter");
            return (SpanExporter) exporterClass.getConstructor(SpanExporter.class).newInstance(exporter);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not initialize the universal profiling host.id exporter", e);
        }
    }

    private static SpanProcessor newUniversalProfilingProcessor(SpanProcessor processor, Resource resource, String socketDir) {
        try {
            Class<?> processorClass = loadUniversalProfilingClass("co.elastic.otel.UniversalProfilingProcessor");
            Method builderMethod = processorClass.getMethod("builder", SpanProcessor.class, Resource.class);
            Object processorBuilder = builderMethod.invoke(null, processor, resource);
            processorBuilder.getClass().getMethod("socketDir", String.class).invoke(processorBuilder, socketDir);
            return (SpanProcessor) processorBuilder.getClass().getMethod("build").invoke(processorBuilder);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e instanceof InvocationTargetException invocationException && invocationException.getCause() != null
                ? invocationException.getCause()
                : e;
            throw new IllegalStateException("Could not initialize universal profiling trace correlation", cause);
        }
    }

    private static Class<?> loadUniversalProfilingClass(String className) throws ClassNotFoundException {
        Class<?> extensionClass = Class.forName(className, true, OtelSdkExportTracerSupplier.class.getClassLoader());
        // The extension is runtime-only because its dependency graph has split packages; reflection needs a dynamic read edge.
        OtelSdkExportTracerSupplier.class.getModule().addReads(extensionClass.getModule());
        return extensionClass;
    }
}

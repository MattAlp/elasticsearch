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
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.otel.pyroscope.PyroscopeOtelConfiguration;
import io.otel.pyroscope.PyroscopeOtelSpanProcessor;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.tracing.APMTracingService;

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
    private boolean pyroscopeStarted;
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
            if (pyroscopeStarted) {
                APMTracingService.stopDemoPyroscopeContext();
                PyroscopeAgent.stop();
                pyroscopeStarted = false;
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
        boolean pyroscopeEnabled = OtelSdkSettings.TELEMETRY_TRACING_PYROSCOPE_ENABLED.get(settings);
        if (pyroscopeEnabled) {
            startPyroscopeProfiler();
        }

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
        OtlpGrpcSpanExporter exporter = builder.build();

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

        var tracerProviderBuilder = SdkTracerProvider.builder().setResource(OtelSdkResource.get(settings)).setSampler(sampler);
        if (pyroscopeEnabled) {
            tracerProviderBuilder.addSpanProcessor(newPyroscopeSpanProcessor());
        }
        SdkTracerProvider tracerProvider = tracerProviderBuilder.addSpanProcessor(processor).build();

        return OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }

    @SuppressWarnings("deprecation")
    private static PyroscopeOtelSpanProcessor newPyroscopeSpanProcessor() {
        PyroscopeOtelConfiguration configuration = new PyroscopeOtelConfiguration.Builder().setRootSpanOnly(false).build();
        return new PyroscopeOtelSpanProcessor(configuration);
    }

    private void startPyroscopeProfiler() {
        Config config = new Config.Builder().setApplicationName(OtelSdkSettings.TELEMETRY_TRACING_PYROSCOPE_APPLICATION_NAME.get(settings))
            .setProfilingEvent(EventType.WALL)
            .setProfilingAlloc("512k")
            .setProfilingLock("10ms")
            .setFormat(Format.JFR)
            .setServerAddress(OtelSdkSettings.TELEMETRY_TRACING_PYROSCOPE_ENDPOINT.get(settings))
            .build();
        PyroscopeAgent.start(config);
        if (PyroscopeAgent.isStarted() == false) {
            throw new IllegalStateException("Pyroscope profiling agent did not start");
        }
        try {
            APMTracingService.startDemoPyroscopeContext();
            pyroscopeStarted = true;
        } catch (RuntimeException | Error failure) {
            PyroscopeAgent.stop();
            throw failure;
        }
    }
}

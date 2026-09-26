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
import io.opentelemetry.api.logs.LoggerProvider;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerBuilder;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;

import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * Applies local capture policy before SDK sampling. Eliding a span borrows its parent's identity rather than
 * creating a dropped child that would turn off downstream sampling or leave an unexported parent in the trace.
 */
final class RecordingPolicyOpenTelemetry implements OpenTelemetry {
    private final OpenTelemetry delegate;
    private final BiPredicate<Context, String> recordingFilter;
    private final TracerProvider tracerProvider = new TracerProvider() {
        @Override
        public Tracer get(String name) {
            return wrap(delegate.getTracerProvider().get(name));
        }

        @Override
        public Tracer get(String name, String version) {
            return wrap(delegate.getTracerProvider().get(name, version));
        }

        @Override
        public TracerBuilder tracerBuilder(String name) {
            TracerBuilder builder = delegate.tracerBuilder(name);
            return new TracerBuilder() {
                @Override
                public TracerBuilder setSchemaUrl(String schemaUrl) {
                    builder.setSchemaUrl(schemaUrl);
                    return this;
                }

                @Override
                public TracerBuilder setInstrumentationVersion(String version) {
                    builder.setInstrumentationVersion(version);
                    return this;
                }

                @Override
                public Tracer build() {
                    return wrap(builder.build());
                }
            };
        }
    };

    RecordingPolicyOpenTelemetry(OpenTelemetry delegate, BiPredicate<Context, String> recordingFilter) {
        this.delegate = delegate;
        this.recordingFilter = recordingFilter;
    }

    @Override
    public TracerProvider getTracerProvider() {
        return tracerProvider;
    }

    @Override
    public MeterProvider getMeterProvider() {
        return delegate.getMeterProvider();
    }

    @Override
    public LoggerProvider getLogsBridge() {
        return delegate.getLogsBridge();
    }

    @Override
    public ContextPropagators getPropagators() {
        return delegate.getPropagators();
    }

    private Tracer wrap(Tracer tracer) {
        return new Tracer() {
            @Override
            public boolean isEnabled() {
                return tracer.isEnabled();
            }

            @Override
            public SpanBuilder spanBuilder(String name) {
                return new PolicySpanBuilder(tracer.spanBuilder(name), name);
            }
        };
    }

    private final class PolicySpanBuilder implements SpanBuilder {
        private final SpanBuilder builder;
        private final String name;
        private Context explicitParent;

        private PolicySpanBuilder(SpanBuilder builder, String name) {
            this.builder = builder;
            this.name = name;
        }

        @Override
        public SpanBuilder setParent(Context context) {
            if (context != null) {
                explicitParent = context;
            }
            builder.setParent(context);
            return this;
        }

        @Override
        public SpanBuilder setNoParent() {
            explicitParent = Context.root();
            builder.setNoParent();
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext context) {
            builder.addLink(context);
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext context, Attributes attributes) {
            builder.addLink(context, attributes);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, String value) {
            builder.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, long value) {
            builder.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, double value) {
            builder.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, boolean value) {
            builder.setAttribute(key, value);
            return this;
        }

        @Override
        public <Value> SpanBuilder setAttribute(AttributeKey<Value> key, Value value) {
            builder.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setSpanKind(SpanKind kind) {
            builder.setSpanKind(kind);
            return this;
        }

        @Override
        public SpanBuilder setStartTimestamp(long timestamp, TimeUnit unit) {
            builder.setStartTimestamp(timestamp, unit);
            return this;
        }

        @Override
        public Span startSpan() {
            Context parent = explicitParent == null ? Context.current() : explicitParent;
            return recordingFilter.test(parent, name) ? builder.startSpan() : Span.wrap(Span.fromContext(parent).getSpanContext());
        }
    }
}

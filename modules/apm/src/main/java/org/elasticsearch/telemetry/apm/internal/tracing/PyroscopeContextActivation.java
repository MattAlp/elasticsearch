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
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.pyroscope.PyroscopeAsyncProfiler;
import io.pyroscope.vendor.one.profiler.AsyncProfiler;

import org.elasticsearch.telemetry.tracing.TracingContext;

final class PyroscopeContextActivation {
    private static volatile AsyncProfiler profiler;

    private PyroscopeContextActivation() {}

    static void install() {
        TracingContext.setContextScopeListener(PyroscopeContextActivation::updateProfilerContext);
    }

    static void start() {
        profiler = PyroscopeAsyncProfiler.getAsyncProfiler();
    }

    static void stop() {
        profiler = null;
        TracingContext.setContextScopeListener(null);
    }

    private static void updateProfilerContext(Context context) {
        AsyncProfiler activeProfiler = profiler;
        if (activeProfiler == null) {
            return;
        }

        SpanContext spanContext = Span.fromContext(context).getSpanContext();
        if (spanContext.isValid() == false) {
            activeProfiler.setTracingContext(0L, 0L);
            activeProfiler.setTraceId(0L, 0L);
            return;
        }

        try {
            String traceId = spanContext.getTraceId();
            activeProfiler.setTracingContext(Long.parseUnsignedLong(spanContext.getSpanId(), 16), 0L);
            activeProfiler.setTraceId(
                Long.parseUnsignedLong(traceId.substring(0, 16), 16),
                Long.parseUnsignedLong(traceId.substring(16), 16)
            );
        } catch (NumberFormatException | IndexOutOfBoundsException failure) {
            activeProfiler.setTracingContext(0L, 0L);
            activeProfiler.setTraceId(0L, 0L);
        }
    }
}

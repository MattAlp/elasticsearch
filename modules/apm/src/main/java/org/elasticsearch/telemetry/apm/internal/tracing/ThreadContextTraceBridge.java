/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;

import java.util.Map;

/**
 * Carries immutable OTel contexts through Elasticsearch's asynchronous {@link ThreadContext} and W3C headers.
 * OTel scopes are only used for synchronous work; they must not be kept open across task boundaries.
 */
final class ThreadContextTraceBridge {

    private static final TextMapGetter<Map<String, String>> HEADER_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private ThreadContextTraceBridge() {}

    static Context localParent(ThreadContext threadContext) {
        return threadContext.getTransient(Task.PARENT_APM_TRACE_CONTEXT);
    }

    static Context remoteParent(ThreadContext threadContext, OpenTelemetry openTelemetry) {
        final String traceParent = threadContext.getTransient(Task.PARENT_TRACE_PARENT_HEADER);
        if (traceParent == null) {
            return null;
        }
        final String traceState = threadContext.getTransient(Task.PARENT_TRACE_STATE);
        final Map<String, String> headers = traceState == null
            ? Map.of(Task.TRACE_PARENT_HTTP_HEADER, traceParent)
            : Map.of(Task.TRACE_PARENT_HTTP_HEADER, traceParent, Task.TRACE_STATE, traceState);
        return openTelemetry.getPropagators().getTextMapPropagator().extract(Context.root(), headers, HEADER_GETTER);
    }

    static void setCurrent(ThreadContext threadContext, OpenTelemetry openTelemetry, Context context) {
        threadContext.putTransient(Task.APM_TRACE_CONTEXT, context);
        openTelemetry.getPropagators().getTextMapPropagator().inject(context, threadContext, (carrier, key, value) -> {
            if (Task.TRACE_PARENT_HTTP_HEADER.equals(key) || Task.TRACE_STATE.equals(key)) {
                carrier.putHeader(key, value);
            }
        });
    }
}

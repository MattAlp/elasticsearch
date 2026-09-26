/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Bridges native OTel contexts and Elasticsearch's transport and asynchronous execution boundaries. */
public final class TracingContext {
    public static final ContextKey<Integer> LOCAL_DEPTH = ContextKey.named("es.apm.span.local_depth");

    private static final List<String> HEADERS = List.of(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE, Task.TRACE_ID);
    private static final List<String> TRANSIENTS = List.of(
        Task.APM_TRACE_CONTEXT,
        Task.PARENT_APM_TRACE_CONTEXT,
        Task.PARENT_TRACE_PARENT_HEADER,
        Task.PARENT_TRACE_STATE
    );
    private static final TextMapGetter<TraceContext> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(TraceContext carrier) {
            return HEADERS;
        }

        @Override
        public String get(TraceContext carrier, String key) {
            String value = carrier.getHeader(key);
            if (value != null) {
                return value;
            }
            return switch (key) {
                case Task.TRACE_PARENT_HTTP_HEADER -> carrier.getTransient(Task.PARENT_TRACE_PARENT_HEADER);
                case Task.TRACE_STATE -> carrier.getTransient(Task.PARENT_TRACE_STATE);
                case Task.TRACE_ID -> null;
                default -> throw new IllegalArgumentException("unexpected trace header [" + key + "]");
            };
        }
    };

    private TracingContext() {}

    /** Native activation takes precedence; legacy and remote contexts are accepted at unmigrated boundaries. */
    public static Context current(TraceContext threadContext) {
        Context active = Context.current();
        if (Span.fromContext(active).getSpanContext().isValid()) {
            return active;
        }
        Object local = threadContext.getTransient(Task.APM_TRACE_CONTEXT);
        if (local == null) {
            local = threadContext.getTransient(Task.PARENT_APM_TRACE_CONTEXT);
        }
        return local instanceof Context context ? context : extract(threadContext);
    }

    /** Incoming requests start from their wire context, never from a reused worker's ambient span. */
    public static Context extract(TraceContext threadContext) {
        return W3CTraceContextPropagator.getInstance().extract(Context.root(), threadContext, GETTER);
    }

    /** Computes depth within this node; a remote parent does not consume the local recording budget. */
    public static int childDepth(Context parent) {
        var spanContext = Span.fromContext(parent).getSpanContext();
        Integer depth = parent.get(LOCAL_DEPTH);
        return spanContext.isValid() && spanContext.isRemote() == false ? (depth == null ? 0 : depth) + 1 : 0;
    }

    /** Retains the parent's context entries while associating a new span with its local depth. */
    public static Context withSpan(Context parent, Span span) {
        if (span.getSpanContext().equals(Span.fromContext(parent).getSpanContext())) {
            return parent.with(span);
        }
        return parent.with(span).with(LOCAL_DEPTH, childDepth(parent));
    }

    /** Borrows a context for one execution slice. Closing restores both contexts and never ends a span. */
    public static Releasable activate(ThreadContext threadContext, Context context) {
        if (context == Context.root() && Context.current() == Context.root()) {
            return () -> {};
        }
        var stored = threadContext.newStoredContextPreservingResponseHeaders(TRANSIENTS, HEADERS);
        Scope scope = null;
        try {
            if (Span.fromContext(context).getSpanContext().isValid()) {
                threadContext.putTransient(Task.APM_TRACE_CONTEXT, context);
                W3CTraceContextPropagator.getInstance().inject(context, threadContext, TraceContext::putHeader);
                threadContext.putHeader(Task.TRACE_ID, Span.fromContext(context).getSpanContext().getTraceId());
            }
            scope = context.makeCurrent();
            Scope activeScope = scope;
            return () -> {
                try {
                    activeScope.close();
                } finally {
                    stored.close();
                }
            };
        } catch (RuntimeException | Error failure) {
            if (scope != null) {
                scope.close();
            }
            stored.close();
            throw failure;
        }
    }

    /** Snapshots wire headers at dispatch time, including unsampled parents and native local instrumentation. */
    public static Map<String, String> headers(Context context, Map<String, String> original) {
        if (Span.fromContext(context).getSpanContext().isValid() == false) {
            return original;
        }
        Map<String, String> headers = new HashMap<>(original);
        headers.remove(Task.TRACE_PARENT_HTTP_HEADER);
        headers.remove(Task.TRACE_STATE);
        W3CTraceContextPropagator.getInstance().inject(context, headers, Map::put);
        headers.put(Task.TRACE_ID, Span.fromContext(context).getSpanContext().getTraceId());
        return headers;
    }
}

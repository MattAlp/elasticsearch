/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.tracing.TracingContext;

/** Instruments coarse ES|QL invocations without collecting query text or creating per-page spans. */
public final class EsqlTracing {
    public static final EsqlTracing NOOP = new EsqlTracing(OpenTelemetry.noop(), null);
    private final boolean enabled;
    private final Tracer tracer;
    private final ThreadContext threadContext;

    /** Uses the node's existing provider; this component never starts or closes an SDK. */
    public EsqlTracing(OpenTelemetry openTelemetry, ThreadContext threadContext) {
        this.tracer = openTelemetry.getTracer("elasticsearch.esql");
        this.enabled = openTelemetry != OpenTelemetry.noop();
        this.threadContext = threadContext;
    }

    /** Starts a phase only for an already sampled query; unrecorded queries keep their parent context. */
    public Span start(String phase) {
        if (enabled == false) {
            return Span.getInvalid();
        }
        Context parent = TracingContext.current(threadContext);
        if (Span.fromContext(parent).getSpanContext().isSampled() == false) {
            return Span.getInvalid();
        }
        return tracer.spanBuilder("esql." + phase).setParent(parent).setAttribute("esql.phase", phase).startSpan();
    }

    /** Borrows a phase for the current slice; completion belongs to its operation, not this scope. */
    public Releasable activate(Span span) {
        return span.getSpanContext().isValid()
            ? TracingContext.activate(threadContext, TracingContext.withSpan(TracingContext.current(threadContext), span))
            : () -> {};
    }

    /** Times synchronous planning, including exceptions, without retaining scope after returning. */
    public <Result, Failure extends Exception> Result phase(String name, CheckedSupplier<Result, Failure> operation) throws Failure {
        Span span = start(name);
        try (var scope = activate(span)) {
            Result result = operation.get();
            span.setAttribute("es.outcome", "success");
            return result;
        } catch (Exception failure) {
            recordFailure(span, failure);
            throw failure;
        } finally {
            span.end();
        }
    }

    /** Ends an asynchronous phase at terminal completion, then restores the caller's context for its continuation. */
    public <Result> void phase(String name, ActionListener<Result> listener, CheckedConsumer<ActionListener<Result>, Exception> operation) {
        Span span = start(name);
        if (span.getSpanContext().isValid() == false) {
            ActionListener.run(listener, operation);
            return;
        }
        var parentListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        var terminal = ActionListener.notifyOnce(ActionListener.<Result>wrap(result -> {
            span.setAttribute("es.outcome", "success");
            span.end();
            parentListener.onResponse(result);
        }, failure -> {
            recordFailure(span, failure);
            span.end();
            parentListener.onFailure(failure);
        }));
        try (var scope = activate(span)) {
            ActionListener.run(ContextPreservingActionListener.wrapPreservingContext(terminal, threadContext), operation);
        }
    }

    /** Records bounded failure metadata; cancellation is an outcome, not a server error. */
    public static void recordFailure(Span span, Exception failure) {
        boolean cancelled = failure instanceof TaskCancelledException;
        span.setAttribute("es.outcome", cancelled ? "cancelled" : "failure");
        span.setAttribute("error.type", failure.getClass().getName());
        if (cancelled == false) {
            span.setStatus(StatusCode.ERROR);
        }
    }
}

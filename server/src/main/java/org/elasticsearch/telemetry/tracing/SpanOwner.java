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
import io.opentelemetry.context.Context;

import java.util.Objects;

/** Owns one framework operation's span across asynchronous execution slices and competing terminal callbacks. */
public final class SpanOwner {
    private volatile Context context = Context.root();
    private boolean attached;
    private boolean finished;
    private TracingContext.Failure failure;
    private String outcome;

    public Context context() {
        return context;
    }

    public synchronized boolean isStarted() {
        return attached || finished;
    }

    /** Attaches at most once, including when completion races with initialization. */
    public synchronized boolean attach(Context parent, Span span) {
        Objects.requireNonNull(span);
        if (attached) {
            span.end();
            return false;
        }
        context = parent.with(span);
        attached = true;
        if (finished) {
            finish(span);
            return false;
        }
        if (failure != null) {
            failure.record(span);
        }
        return true;
    }

    /** Uninstrumented work propagates causality but never gains ownership of its parent's span. */
    public synchronized void borrow(Context parent) {
        assert attached == false;
        Objects.requireNonNull(parent);
        var spanContext = Span.fromContext(parent).getSpanContext();
        context = spanContext.isValid() ? parent.with(Span.wrap(spanContext)) : parent;
    }

    /** Must be called before terminal cleanup, including failures returned inside successful response envelopes. */
    public synchronized void fail(Throwable cause) {
        if (finished == false && failure == null) {
            failure = TracingContext.failure(cause);
            if (attached) {
                failure.record(Span.fromContext(context));
            }
        }
    }

    /** Ends only the owned span; duplicate completion and borrowed contexts are harmless. */
    public synchronized void end(String outcome) {
        if (finished == false) {
            finished = true;
            this.outcome = outcome;
            if (attached) {
                finish(Span.fromContext(context));
            }
        }
    }

    private void finish(Span span) {
        if (failure != null) {
            failure.record(span);
        } else if (outcome != null) {
            span.setAttribute("es.outcome", outcome);
        }
        span.end();
    }
}

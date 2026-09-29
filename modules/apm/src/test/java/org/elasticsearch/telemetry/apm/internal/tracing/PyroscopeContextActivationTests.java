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
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.elasticsearch.telemetry.tracing.TracingContext;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

/** Verifies active profile labels follow native context activation and restoration. */
public class PyroscopeContextActivationTests extends ESTestCase {

    public void testNestedScopesRestoreTheParent() {
        List<String> appliedSpanIds = new ArrayList<>();
        TracingContext.setContextScopeListener(context -> appliedSpanIds.add(spanId(context)));
        Context parent = contextWithSpan("00000000000000000000000000000001", "0000000000000001");
        Context child = contextWithSpan("00000000000000000000000000000001", "0000000000000002");

        try {
            try (Scope parentScope = TracingContext.makeCurrent(parent)) {
                assertThat(spanId(Context.current()), equalTo("0000000000000001"));
                try (Scope childScope = TracingContext.makeCurrent(child)) {
                    assertThat(spanId(Context.current()), equalTo("0000000000000002"));
                }
                assertThat(spanId(Context.current()), equalTo("0000000000000001"));
            }

            assertThat(spanId(Context.current()), equalTo("none"));
            assertThat(appliedSpanIds, equalTo(List.of("0000000000000001", "0000000000000002", "0000000000000001", "none")));
        } finally {
            TracingContext.setContextScopeListener(null);
        }
    }

    public void testScopeOnWorkerThreadCarriesAndClearsTheContext() throws InterruptedException {
        List<String> appliedSpanIds = Collections.synchronizedList(new ArrayList<>());
        TracingContext.setContextScopeListener(context -> appliedSpanIds.add(spanId(context)));
        Context parent = contextWithSpan("00000000000000000000000000000003", "0000000000000003");
        AtomicReference<String> workerSpanId = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try (Scope scope = TracingContext.makeCurrent(parent)) {
                workerSpanId.set(spanId(Context.current()));
            }
        });

        try {
            worker.start();
            worker.join();
            assertThat(workerSpanId.get(), equalTo("0000000000000003"));
            assertThat(appliedSpanIds, equalTo(List.of("0000000000000003", "none")));
        } finally {
            TracingContext.setContextScopeListener(null);
        }
    }

    private static Context contextWithSpan(String traceId, String spanId) {
        SpanContext spanContext = SpanContext.create(traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault());
        return Context.root().with(Span.wrap(spanContext));
    }

    private static String spanId(Context context) {
        SpanContext spanContext = Span.fromContext(context).getSpanContext();
        return spanContext.isValid() ? spanContext.getSpanId() : "none";
    }
}

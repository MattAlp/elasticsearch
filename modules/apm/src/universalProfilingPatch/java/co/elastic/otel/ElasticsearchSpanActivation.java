/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package co.elastic.otel;

import io.opentelemetry.api.trace.Span;

import java.util.function.Consumer;

/**
 * Publishes the span activated by Elasticsearch to the profiler's thread-local correlation storage.
 */
public final class ElasticsearchSpanActivation implements Consumer<Span> {

    @Override
    public void accept(Span span) {
        ProfilerSharedMemoryWriter.updateThreadCorrelationStorage(span);
    }
}

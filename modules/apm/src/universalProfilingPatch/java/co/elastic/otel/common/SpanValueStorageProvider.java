/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package co.elastic.otel.common;

import io.opentelemetry.api.trace.Span;

import com.blogspot.mydailyjava.weaklockfree.WeakConcurrentMap;

/**
 * Stores profiling correlation values without patching OpenTelemetry's {@code SdkSpan}.
 */
public interface SpanValueStorageProvider {

    static SpanValueStorageProvider get() {
        return MapBacked.getInstance();
    }

    SpanValueStorage get(Span span, boolean initialize);

    class MapBacked implements SpanValueStorageProvider {

        private static MapBacked instance;

        static synchronized MapBacked getInstance() {
            if (instance == null) {
                instance = new MapBacked();
            }
            return instance;
        }

        private final WeakConcurrentMap<Span, SpanValueStorage> storageMap = WeakConcurrent.createMap();

        @Override
        public SpanValueStorage get(Span span, boolean initialize) {
            SpanValueStorage storage = storageMap.get(span);
            if (storage == null && initialize) {
                storageMap.putIfAbsent(span, new SpanValueStorage());
                storage = storageMap.get(span);
            }
            return storage;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Provides a way to access {@link ComputeSearchContext}s by shard ID (i.e., {@link IndexedByShardId}), both as a global view (all
 * contexts, used by the node-reduce driver) and individual slices (used by the data drivers). Contexts are added to the array in batches
 * via {@link #newSubRangeView(List)}, which also returns an {@link IndexedByShardId} of the slice.
 */
class AcquiredSearchContexts implements Releasable {
    private final List<ComputeSearchContext> allContexts;
    private int nextAddIndex = 0;
    private boolean isClosed = false;

    AcquiredSearchContexts(int size) {
        this.allContexts = new ArrayList<>(size);
    }

    public synchronized boolean isEmpty() {
        return nextAddIndex == 0;
    }

    public synchronized IndexedByShardId<ComputeSearchContext> globalView() {
        return new GlobalView();
    }

    /**
    * Adds new contexts and returns a slice of the global array containing only the added elements. Note that the slice is still indexed by
     * the global indices, i.e., the first element in the returned slice might not be at index 0.
    * This has consequences:
    * <ol>
    * <li>{@link IndexedByShardId#get} fails if the index is out of the slice range.</li>
    * <li>{@link IndexedByShardId#iterable} only returns the elements in the specified range.</li>
    * <li>{@link IndexedByShardId#map} creates a cache in the specified range size.</li>
    * </ol>
    */
    public synchronized IndexedByShardId<ComputeSearchContext> newSubRangeView(List<SearchContext> searchContexts) {
        checkNotClosed();
        var startingIndex = nextAddIndex;
        for (var cse : searchContexts) {
            allContexts.add(new ComputeSearchContext(nextAddIndex, cse));
            nextAddIndex++;
        }
        return new SubRanged<>(allContexts, startingIndex, nextAddIndex);
    }

    /**
     * All of these methods are synchronized on the outer instance, since they access the shared mutable array, which may be modified by
     * other threads calling {@link #newSubRangeView(List)}. However, since this is basically only used by a single thread, there isn't any
     * actual contention.
     */
    private class GlobalView implements IndexedByShardId<ComputeSearchContext> {
        @Override
        public ComputeSearchContext get(int shardId) {
            synchronized (AcquiredSearchContexts.this) {
                checkNotClosed();
                if (shardId < 0 || shardId >= nextAddIndex) {
                    throw new IndexOutOfBoundsException("shardId " + shardId + " out of bounds [0, " + nextAddIndex + ")");
                }
                return allContexts.get(shardId);
            }
        }

        @Override
        public Iterable<ComputeSearchContext> iterable() {
            synchronized (AcquiredSearchContexts.this) {
                checkNotClosed();
                return snapshotRange(allContexts, 0, nextAddIndex);
            }
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<ComputeSearchContext, S> mapper) {
            synchronized (AcquiredSearchContexts.this) {
                checkNotClosed();
                return new Mapped<>(this, allContexts.size(), 0, mapper);
            }
        }

        @Override
        public int size() {
            synchronized (AcquiredSearchContexts.this) {
                checkNotClosed();
                return nextAddIndex;
            }
        }
    }

    private void checkNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("ComputeSearchContexts is already closed");
        }
    }

    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }
        Releasables.close(allContexts);
        isClosed = true;
    }

    /**
     * This class doesn't need synchronization because it's created from a {@code synchronized} context ({@link #newSubRangeView(List)}) and
     * it doesn't read past its initial bounds, so it's safe to read from the global array.
     */
    private static class SubRanged<T> implements IndexedByShardId<T> {
        private final List<T> list;
        private final int from;
        private final int to;

        SubRanged(List<T> list, int from, int to) {
            this.list = list;
            this.from = from;
            this.to = to;
        }

        @Override
        public T get(int shardId) {
            if (shardId < from || shardId >= to) {
                throw new IndexOutOfBoundsException("shardId " + shardId + " out of bounds [" + from + ", " + to + ")");
            }

            return list.get(shardId);
        }

        @Override
        public Iterable<? extends T> iterable() {
            return snapshotRange(list, from, to);
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
            return new Mapped<>(this, to - from, from, mapper);
        }
    }

    private static <T> List<T> snapshotRange(List<T> list, int from, int to) {
        List<T> snapshot = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            snapshot.add(list.get(i));
        }
        return snapshot;
    }

    // This class doesn't need to be synchronized since it delegates to the underlying IndexedByShardId, which is assumed to be thread-safe.
    private static class Mapped<T, S> implements IndexedByShardId<S> {
        private final IndexedByShardId<T> original;
        private final List<S> cache;
        private final int offset;
        private final Function<T, S> mapper;

        Mapped(IndexedByShardId<T> original, int size, int offset, Function<T, S> mapper) {
            this.original = original;
            this.mapper = mapper;
            this.cache = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                cache.add(null);
            }
            this.offset = offset;
        }

        @Override
        public S get(int shardId) {
            var fixedShardId = shardId - offset;
            ensureCacheCapacity(fixedShardId + 1);
            if (cache.get(fixedShardId) == null) {
                synchronized (this) {
                    ensureCacheCapacity(fixedShardId + 1);
                    if (cache.get(fixedShardId) == null) {
                        cache.set(fixedShardId, mapper.apply(original.get(shardId)));
                    }
                }
            }
            return cache.get(fixedShardId);
        }

        @Override
        public Iterable<? extends S> iterable() {
            return IntStream.range(offset, original.size() + offset).mapToObj(this::get).toList();
        }

        @Override
        public int size() {
            return original.size();
        }

        @Override
        public <U> IndexedByShardId<U> map(Function<S, U> anotherMapper) {
            return new Mapped<>(this, cache.size(), offset, anotherMapper);
        }

        private void ensureCacheCapacity(int size) {
            while (cache.size() < size) {
                cache.add(null);
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Releasable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Retains {@link AcquiredSearchContexts} beyond the lifetime of the initial distributed query so a follow-up fetch
 * phase can revisit the original shard owners.
 * <p>
 * A {@link Registration} keeps a session discoverable for new continuation requests. Once the registration is closed,
 * no new leases may be acquired, but already acquired {@link Lease}s remain valid until they are closed.
 */
final class RetainedSearchContextsRegistry {
    private final Map<String, Entry> entries = new HashMap<>();

    synchronized Registration register(String sessionId, AcquiredSearchContexts searchContexts) {
        if (entries.containsKey(sessionId)) {
            throw new IllegalStateException("search contexts already retained for session [" + sessionId + "]");
        }
        Entry entry = new Entry(searchContexts);
        entries.put(sessionId, entry);
        return new Registration(sessionId, searchContexts.globalView(), () -> releaseRegistration(sessionId, entry));
    }

    synchronized Lease acquire(String sessionId) {
        Entry entry = entries.get(sessionId);
        if (entry == null || entry.registrationOpen == false) {
            throw new IllegalStateException("no retained search contexts for session [" + sessionId + "]");
        }
        entry.refCount++;
        return new Lease(sessionId, entry.searchContexts.globalView(), () -> releaseLease(sessionId, entry));
    }

    synchronized int retainedSessions() {
        return entries.size();
    }

    synchronized boolean isRegistered(String sessionId) {
        Entry entry = entries.get(sessionId);
        return entry != null && entry.registrationOpen;
    }

    synchronized void closeRegistration(String sessionId) {
        Entry entry = entries.get(sessionId);
        if (entry != null) {
            releaseRegistration(sessionId, entry);
        }
    }

    private synchronized void releaseRegistration(String sessionId, Entry entry) {
        if (entry.registrationOpen == false) {
            return;
        }
        entry.registrationOpen = false;
        releaseRef(sessionId, entry);
    }

    private synchronized void releaseLease(String sessionId, Entry entry) {
        releaseRef(sessionId, entry);
    }

    private void releaseRef(String sessionId, Entry entry) {
        entry.refCount--;
        if (entry.refCount < 0) {
            throw new IllegalStateException("reference count underflow for retained session [" + sessionId + "]");
        }
        if (entry.refCount == 0) {
            entries.remove(sessionId, entry);
            entry.searchContexts.close();
        }
    }

    private static final class Entry {
        private final AcquiredSearchContexts searchContexts;
        private int refCount = 1;
        private boolean registrationOpen = true;

        private Entry(AcquiredSearchContexts searchContexts) {
            this.searchContexts = searchContexts;
        }
    }

    static final class Registration implements Releasable {
        private final String sessionId;
        private final IndexedByShardId<ComputeSearchContext> searchContexts;
        private final Runnable onClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Registration(String sessionId, IndexedByShardId<ComputeSearchContext> searchContexts, Runnable onClose) {
            this.sessionId = sessionId;
            this.searchContexts = searchContexts;
            this.onClose = onClose;
        }

        String sessionId() {
            return sessionId;
        }

        IndexedByShardId<ComputeSearchContext> searchContexts() {
            return searchContexts;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }
    }

    static final class Lease implements Releasable {
        private final String sessionId;
        private final IndexedByShardId<ComputeSearchContext> searchContexts;
        private final Runnable onClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(String sessionId, IndexedByShardId<ComputeSearchContext> searchContexts, Runnable onClose) {
            this.sessionId = sessionId;
            this.searchContexts = searchContexts;
            this.onClose = onClose;
        }

        String sessionId() {
            return sessionId;
        }

        IndexedByShardId<ComputeSearchContext> searchContexts() {
            return searchContexts;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }
    }
}

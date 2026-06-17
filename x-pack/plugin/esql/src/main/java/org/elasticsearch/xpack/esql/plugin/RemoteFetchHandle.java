/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Serialized row handle for coordinator-driven remote fetch.
 * <p>
 * {@code DocBlock} references (shard ordinal, segment, doc) are only meaningful on the node that produced them -
 * a coordinator receiving pages from multiple data nodes cannot resolve those local ordinals back to the original
 * shard. This handle pairs the doc triple with the originating node and retained session so the coordinator can route a
 * fetch request back to the owning node after narrowing the candidate set.
 * <p>
 * Remote fetch now carries these values in an internal {@code CompositeBlock} struct-of-arrays representation
 * ({@link RemoteFetchHandleBlock}) so exchange pages avoid per-row blob encoding/decoding. The bytes form remains
 * available for backwards-compatible test fixtures and transitional readers.
 * <p>
 * This is intentionally a bytes payload contract for compute pages, not a transport-level wire contract. If later
 * phases need explicit cross-cluster routing identity in transport-scoped handle fields, introduce an explicit
 * versioned transport representation then.
 */
public record RemoteFetchHandle(String nodeId, String retainedSessionId, int shard, int segment, int doc) {
    public RemoteFetchHandle {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(retainedSessionId, "retainedSessionId");
        // Derived from DocVector ordinals in normal execution; keep as assert to avoid per-row checks on hot paths.
        assert shard >= 0 : "shard must be non-negative but was [" + shard + "]";
        assert segment >= 0 : "segment must be non-negative but was [" + segment + "]";
        assert doc >= 0 : "doc must be non-negative but was [" + doc + "]";
    }

    public static void encodeTo(BytesStreamOutput out, String nodeId, String retainedSessionId, int shard, int segment, int doc)
        throws IOException {
        out.writeString(nodeId);
        out.writeString(retainedSessionId);
        out.writeVInt(shard);
        out.writeVInt(segment);
        out.writeVInt(doc);
    }

    /**
     * Encodes the handle into a transport-safe {@link BytesRef} so it can be carried through regular page blocks.
     */
    public BytesRef toBytesRef() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            encodeTo(out, nodeId, retainedSessionId, shard, segment, doc);
            return BytesRef.deepCopyOf(out.bytes().toBytesRef());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode remote fetch handle", e);
        }
    }

    public static RemoteFetchHandle fromBytesRef(BytesRef bytesRef) {
        try (StreamInput in = StreamInput.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length)) {
            return new RemoteFetchHandle(in.readString(), in.readString(), in.readVInt(), in.readVInt(), in.readVInt());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode remote fetch handle", e);
        }
    }
}

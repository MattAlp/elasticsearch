/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes and decodes remote fetch handles as a struct-of-arrays composite block.
 * <p>
 * Layout:
 * <ol>
 *     <li>node id ({@link BytesRefBlock})</li>
 *     <li>retained session id ({@link BytesRefBlock})</li>
 *     <li>shard ordinal ({@link IntBlock})</li>
 *     <li>segment ordinal ({@link IntBlock})</li>
 *     <li>doc id ({@link IntBlock})</li>
 * </ol>
 */
public final class RemoteFetchHandleBlock {
    private static final int NODE_ID_BLOCK = 0;
    private static final int SESSION_ID_BLOCK = 1;
    private static final int SHARD_BLOCK = 2;
    private static final int SEGMENT_BLOCK = 3;
    private static final int DOC_BLOCK = 4;
    private static final int BLOCK_COUNT = 5;

    private RemoteFetchHandleBlock() {}

    public static Block fromDocVector(BlockFactory blockFactory, DocVector docs, String nodeId, String retainedSessionId) {
        try (
            BytesRefBlock.Builder nodeIds = blockFactory.newBytesRefBlockBuilder(docs.getPositionCount());
            BytesRefBlock.Builder sessions = blockFactory.newBytesRefBlockBuilder(docs.getPositionCount());
            IntBlock.Builder shards = blockFactory.newIntBlockBuilder(docs.getPositionCount());
            IntBlock.Builder segments = blockFactory.newIntBlockBuilder(docs.getPositionCount());
            IntBlock.Builder docIds = blockFactory.newIntBlockBuilder(docs.getPositionCount())
        ) {
            BytesRef node = new BytesRef(nodeId);
            BytesRef session = new BytesRef(retainedSessionId);
            for (int position = 0; position < docs.getPositionCount(); position++) {
                nodeIds.appendBytesRef(node);
                sessions.appendBytesRef(session);
                shards.appendInt(docs.shards().getInt(position));
                segments.appendInt(docs.segments().getInt(position));
                docIds.appendInt(docs.docs().getInt(position));
            }
            return buildComposite(nodeIds.build(), sessions.build(), shards.build(), segments.build(), docIds.build());
        }
    }

    public static Block fromHandles(BlockFactory blockFactory, List<RemoteFetchHandle> handles) {
        try (
            BytesRefBlock.Builder nodeIds = blockFactory.newBytesRefBlockBuilder(handles.size());
            BytesRefBlock.Builder sessions = blockFactory.newBytesRefBlockBuilder(handles.size());
            IntBlock.Builder shards = blockFactory.newIntBlockBuilder(handles.size());
            IntBlock.Builder segments = blockFactory.newIntBlockBuilder(handles.size());
            IntBlock.Builder docIds = blockFactory.newIntBlockBuilder(handles.size())
        ) {
            for (RemoteFetchHandle handle : handles) {
                nodeIds.appendBytesRef(new BytesRef(handle.nodeId()));
                sessions.appendBytesRef(new BytesRef(handle.retainedSessionId()));
                shards.appendInt(handle.shard());
                segments.appendInt(handle.segment());
                docIds.appendInt(handle.doc());
            }
            return buildComposite(nodeIds.build(), sessions.build(), shards.build(), segments.build(), docIds.build());
        }
    }

    public static List<RemoteFetchHandle> decodeHandles(Block handlesBlock, int positionCount) {
        if (handlesBlock instanceof CompositeBlock compositeBlock) {
            return decodeCompositeHandles(compositeBlock, positionCount);
        }
        if (handlesBlock instanceof BytesRefBlock bytesRefBlock) {
            // Backward-compatible decoder for pre-composite test inputs.
            return decodeSerializedHandles(bytesRefBlock, positionCount);
        }
        throw new IllegalStateException(
            "remote fetch handle column must be CompositeBlock or BytesRefBlock but was [" + handlesBlock.getClass().getSimpleName() + "]"
        );
    }

    private static List<RemoteFetchHandle> decodeSerializedHandles(BytesRefBlock handlesBlock, int positionCount) {
        List<RemoteFetchHandle> handles = new ArrayList<>(positionCount);
        BytesRef scratch = new BytesRef();
        for (int position = 0; position < positionCount; position++) {
            if (handlesBlock.isNull(position)) {
                throw new IllegalStateException("remote fetch handle column cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("remote fetch handle column must contain exactly one handle per row");
            }
            handles.add(RemoteFetchHandle.fromBytesRef(handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)));
        }
        return handles;
    }

    private static List<RemoteFetchHandle> decodeCompositeHandles(CompositeBlock handlesBlock, int positionCount) {
        if (handlesBlock.getBlockCount() != BLOCK_COUNT) {
            throw new IllegalStateException(
                "remote fetch composite handle block must contain ["
                    + BLOCK_COUNT
                    + "] columns but found ["
                    + handlesBlock.getBlockCount()
                    + "]"
            );
        }
        BytesRefBlock nodeIds = expect(handlesBlock.getBlock(NODE_ID_BLOCK), BytesRefBlock.class, "node id");
        BytesRefBlock sessions = expect(handlesBlock.getBlock(SESSION_ID_BLOCK), BytesRefBlock.class, "retained session id");
        IntBlock shards = expect(handlesBlock.getBlock(SHARD_BLOCK), IntBlock.class, "shard");
        IntBlock segments = expect(handlesBlock.getBlock(SEGMENT_BLOCK), IntBlock.class, "segment");
        IntBlock docs = expect(handlesBlock.getBlock(DOC_BLOCK), IntBlock.class, "doc");

        List<RemoteFetchHandle> handles = new ArrayList<>(positionCount);
        BytesRef nodeScratch = new BytesRef();
        BytesRef sessionScratch = new BytesRef();
        for (int position = 0; position < positionCount; position++) {
            String nodeId = bytesRefAt(nodeIds, position, "node id", nodeScratch);
            String retainedSessionId = bytesRefAt(sessions, position, "retained session id", sessionScratch);
            int shard = intAt(shards, position, "shard");
            int segment = intAt(segments, position, "segment");
            int doc = intAt(docs, position, "doc");
            handles.add(new RemoteFetchHandle(nodeId, retainedSessionId, shard, segment, doc));
        }
        return handles;
    }

    private static int intAt(IntBlock block, int position, String columnName) {
        if (block.isNull(position)) {
            throw new IllegalStateException("remote fetch handle [" + columnName + "] column cannot contain nulls");
        }
        if (block.getValueCount(position) != 1) {
            throw new IllegalStateException("remote fetch handle [" + columnName + "] column must contain exactly one value per row");
        }
        return block.getInt(block.getFirstValueIndex(position));
    }

    private static String bytesRefAt(BytesRefBlock block, int position, String columnName, BytesRef scratch) {
        if (block.isNull(position)) {
            throw new IllegalStateException("remote fetch handle [" + columnName + "] column cannot contain nulls");
        }
        if (block.getValueCount(position) != 1) {
            throw new IllegalStateException("remote fetch handle [" + columnName + "] column must contain exactly one value per row");
        }
        return block.getBytesRef(block.getFirstValueIndex(position), scratch).utf8ToString();
    }

    private static CompositeBlock buildComposite(Block nodeIds, Block sessions, Block shards, Block segments, Block docs) {
        Block[] blocks = new Block[] { nodeIds, sessions, shards, segments, docs };
        boolean success = false;
        try {
            CompositeBlock result = new CompositeBlock(blocks);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private static <T extends Block> T expect(Block block, Class<T> type, String columnName) {
        if (type.isInstance(block)) {
            return type.cast(block);
        }
        throw new IllegalStateException(
            "remote fetch handle ["
                + columnName
                + "] column must be ["
                + type.getSimpleName()
                + "] but was ["
                + block.getClass().getSimpleName()
                + "]"
        );
    }
}

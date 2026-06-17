/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchHandleBlockTests extends ESTestCase {
    public void testRoundTripCompositeHandles() {
        List<RemoteFetchHandle> expected = List.of(
            new RemoteFetchHandle("node-a", "session-a", 1, 2, 3),
            new RemoteFetchHandle("node-b", "session-b", 4, 5, 6)
        );

        try (var handlesBlock = RemoteFetchHandleBlock.fromHandles(TestBlockFactory.getNonBreakingInstance(), expected)) {
            assertThat(RemoteFetchHandleBlock.decodeHandles(handlesBlock, expected.size()), equalTo(expected));
        }
    }

    public void testFromDocVectorRoundTrip() {
        try (DocBlock.Builder docs = DocBlock.newBlockBuilder(TestBlockFactory.getNonBreakingInstance(), 2)) {
            docs.appendShard(1).appendSegment(10).appendDoc(100);
            docs.appendShard(2).appendSegment(20).appendDoc(200);
            try (
                DocBlock docBlock = docs.build();
                var handlesBlock = RemoteFetchHandleBlock.fromDocVector(
                    TestBlockFactory.getNonBreakingInstance(),
                    docBlock.asVector(),
                    "node-a",
                    "session-a"
                )
            ) {
                assertThat(
                    RemoteFetchHandleBlock.decodeHandles(handlesBlock, 2),
                    equalTo(
                        List.of(
                            new RemoteFetchHandle("node-a", "session-a", 1, 10, 100),
                            new RemoteFetchHandle("node-a", "session-a", 2, 20, 200)
                        )
                    )
                );
            }
        }
    }

    public void testDecodeSerializedBytesFallback() {
        List<RemoteFetchHandle> expected = List.of(new RemoteFetchHandle("node-a", "session-a", 1, 2, 3));
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(expected.size())) {
            builder.appendBytesRef(expected.getFirst().toBytesRef());
            try (BytesRefBlock bytesRefBlock = builder.build()) {
                assertThat(RemoteFetchHandleBlock.decodeHandles(bytesRefBlock, expected.size()), equalTo(expected));
            }
        }
    }
}

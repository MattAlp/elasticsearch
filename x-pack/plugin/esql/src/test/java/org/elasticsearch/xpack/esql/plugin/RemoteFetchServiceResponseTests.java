/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RemoteFetchServiceResponseTests extends AbstractWireSerializingTestCase<RemoteFetchService.Response> {
    private final List<CircuitBreaker> breakers = new ArrayList<>();

    @Override
    protected Writeable.Reader<RemoteFetchService.Response> instanceReader() {
        return in -> new RemoteFetchService.Response(in, TestBlockFactory.getNonBreakingInstance());
    }

    @Override
    protected RemoteFetchService.Response createTestInstance() {
        return new RemoteFetchService.Response(
            randomList(0, 10, () -> randomPage(TestBlockFactory.getNonBreakingInstance())),
            TestBlockFactory.getNonBreakingInstance()
        );
    }

    @Override
    protected RemoteFetchService.Response mutateInstance(RemoteFetchService.Response instance) throws IOException {
        List<Page> pages = instance.pages() == null ? List.of() : new ArrayList<>(instance.pages());
        if (randomBoolean()) {
            pages.add(randomPage(TestBlockFactory.getNonBreakingInstance()));
        } else if (pages.isEmpty() == false) {
            pages.removeLast();
        } else {
            pages = List.of(randomPage(TestBlockFactory.getNonBreakingInstance()));
        }
        return new RemoteFetchService.Response(pages, TestBlockFactory.getNonBreakingInstance());
    }

    public void testWithBreaker() throws IOException {
        BlockFactory origFactory = blockFactory();
        BlockFactory copyFactory = blockFactory();
        RemoteFetchService.Response orig = new RemoteFetchService.Response(randomList(0, 5, () -> randomPage(origFactory)), origFactory);
        try {
            RemoteFetchService.Response copy = copyInstance(
                orig,
                getNamedWriteableRegistry(),
                (out, value) -> value.writeTo(out),
                in -> new RemoteFetchService.Response(in, copyFactory),
                TransportVersion.current()
            );
            try {
                assertThat(copy, equalTo(orig));
            } finally {
                copy.decRef();
            }
            assertThat(copyFactory.breaker().getUsed(), equalTo(0L));
        } finally {
            orig.decRef();
        }
        assertThat(origFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testTakePages() {
        BlockFactory factory = blockFactory();
        RemoteFetchService.Response response = new RemoteFetchService.Response(randomList(0, 5, () -> randomPage(factory)), factory);
        try {
            List<Page> pages = response.takePages();
            if (pages != null) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
            }
            assertThat(response.takePages(), nullValue());
            assertThat(factory.breaker().getUsed(), equalTo(0L));
        } finally {
            response.decRef();
        }
    }

    private Page randomPage(BlockFactory blockFactory) {
        Block[] blocks = new Block[between(1, 5)];
        int positionCount = between(1, 50);
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory,
                    randomFrom(
                        ElementType.BOOLEAN,
                        ElementType.BYTES_REF,
                        ElementType.DOUBLE,
                        ElementType.FLOAT,
                        ElementType.INT,
                        ElementType.LONG
                    ),
                    positionCount,
                    randomBoolean(),
                    1,
                    1,
                    0,
                    0
                ).block();
            }
            return new Page(blocks);
        } finally {
            if (blocks[blocks.length - 1] == null) {
                Releasables.close(blocks);
            }
        }
    }

    private BlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(4)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return BlockFactory.builder(bigArrays).build();
    }

    @After
    public void allBreakersEmpty() throws Exception {
        MockBigArrays.ensureAllArraysAreReleased();
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}

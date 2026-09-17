/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExchangeResponseTests extends ComputeTestCase {

    public void testReverseBytesWhenSerializing() throws Exception {
        BlockFactory factory = blockFactory();
        int numBlocks = between(1, 10);
        Block[] blocks = new Block[numBlocks];
        int positions = randomIntBetween(1, 10);
        for (int b = 0; b < numBlocks; b++) {
            var block = RandomBlock.randomBlock(
                factory,
                randomFrom(ElementType.BOOLEAN, ElementType.LONG, ElementType.BYTES_REF),
                positions,
                randomBoolean(),
                1,
                5,
                0,
                1
            );
            blocks[b] = block.block();
        }
        Page page = new Page(blocks);
        AtomicLong serializedBytes = new AtomicLong();
        ExchangeResponse response = new ExchangeResponse(factory, page, randomBoolean(), serializedBytes::addAndGet);
        long beforeUsage = factory.breaker().getUsed();
        assertThat(page.ramBytesUsedByBlocks(), equalTo(beforeUsage));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            assertThat(serializedBytes.get(), equalTo(output.position()));
        }
        long afterUsage = factory.breaker().getUsed();
        assertThat(afterUsage, equalTo(beforeUsage * 2L));
        response.close();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testProfileCompletionWaitsForLastSerializedResponse() throws Exception {
        BlockFactory factory = blockFactory();
        ExchangeSinkHandler handler = new ExchangeSinkHandler(factory, 1, System::currentTimeMillis);
        handler.enableProfiling();
        ExchangeSink sink = handler.createExchangeSink(() -> {});
        sink.addPage(new Page(factory.newConstantIntBlockWith(1, 10)));
        sink.finish();

        PlainActionFuture<Void> completion = new PlainActionFuture<>();
        handler.addProfileCompletionListener(completion);
        AtomicReference<ExchangeResponse> retainedResponse = new AtomicReference<>();
        AtomicReference<Exception> serializationFailure = new AtomicReference<>();
        AtomicLong serializedBytes = new AtomicLong();
        handler.fetchPageAsync(false, ActionListener.wrap(response -> {
            response.incRef();
            retainedResponse.set(response);
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                response.writeTo(output);
                serializedBytes.set(output.position());
            } catch (Exception e) {
                serializationFailure.set(e);
            }
        }, serializationFailure::set));

        handler.fetchPageAsync(false, ActionListener.noop());
        assertFalse(completion.isDone());
        assertThat(serializationFailure.get(), nullValue());
        assertThat(handler.profile(), equalTo(new ExchangeSinkHandler.Profile(1L, 10L, serializedBytes.get())));

        retainedResponse.get().close();
        completion.actionGet();
        assertThat(handler.profile(), equalTo(new ExchangeSinkHandler.Profile(1L, 10L, serializedBytes.get())));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.After;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchServiceTests extends MapperServiceTestCase {
    private Directory directory;
    private IndexReader reader;
    private BlockFactory blockFactory;

    public void testInputPagePreservesHandleCoordinates() {
        blockFactory = blockFactory();
        List<RemoteFetchHandle> handles = List.of(
            new RemoteFetchHandle("node-1", "session-1", 3, 7, 11),
            new RemoteFetchHandle("node-1", "session-1", 5, 13, 17)
        );

        Page page = RemoteFetchService.inputPage(blockFactory, handles);
        try {
            DocBlock docBlock = page.getBlock(0);
            assertThat(docBlock.asVector().shards().getInt(0), equalTo(3));
            assertThat(docBlock.asVector().segments().getInt(0), equalTo(7));
            assertThat(docBlock.asVector().docs().getInt(0), equalTo(11));
            assertThat(docBlock.asVector().shards().getInt(1), equalTo(5));
            assertThat(docBlock.asVector().segments().getInt(1), equalTo(13));
            assertThat(docBlock.asVector().docs().getInt(1), equalTo(17));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testExecuteFetchLoadsRequestedFieldsInHandleOrder() throws Exception {
        blockFactory = blockFactory();
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("k").field("type", "keyword").endObject();
            b.startObject("n").field("type", "long").endObject();
        }));

        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            addDoc(writer, "k0", 10);
            addDoc(writer, "k1", 20);
            addDoc(writer, "k2", 30);
        }
        reader = DirectoryReader.open(directory);

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService, null);
        EsPhysicalOperationProviders.DefaultShardContext fetchShardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = RemoteFetchService.buildFieldInfos(
            List.of(new RemoteFetchService.FetchField("k", DataType.KEYWORD), new RemoteFetchService.FetchField("n", DataType.LONG)),
            new IndexedByShardIdFromSingleton<>(fetchShardContext),
            PlannerSettings.DEFAULTS
        );

        List<Page> pages = RemoteFetchService.executeFetch(
            List.of(
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 2),
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 0),
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 1)
            ),
            fieldInfos,
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(reader, sourcePaths -> SourceLoader.FROM_STORED_SOURCE, 0.2)
            ),
            BigArrays.NON_RECYCLING_INSTANCE,
            blockFactory,
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS,
            PlannerSettings.DEFAULTS
        );

        try {
            assertThat(pages.size(), equalTo(1));
            Page page = pages.getFirst();
            BytesRef scratch = new BytesRef();
            assertThat(page.getBlockCount(), equalTo(2));
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(0, scratch).utf8ToString(),
                equalTo("k2")
            );
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(1, scratch).utf8ToString(),
                equalTo("k0")
            );
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(2, scratch).utf8ToString(),
                equalTo("k1")
            );
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(0), equalTo(30L));
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(1), equalTo(10L));
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(2), equalTo(20L));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
        }
    }

    private static void addDoc(IndexWriter writer, String keyword, long number) throws IOException {
        Document document = new Document();
        document.add(new SortedDocValuesField("k", new BytesRef(keyword)));
        document.add(new NumericDocValuesField("n", number));
        writer.addDocument(document);
    }

    private BlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(4)).withCircuitBreaking();
        return BlockFactory.builder(bigArrays).build();
    }

    @After
    public void tearDownResources() throws Exception {
        IOUtils.close(reader, directory);
        if (blockFactory != null) {
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
            assertThat(blockFactory.breaker().getTrippedCount(), equalTo(0L));
            assertThat(blockFactory.breaker().getName(), equalTo(CircuitBreaker.REQUEST));
        }
        MockBigArrays.ensureAllArraysAreReleased();
        super.tearDown();
    }
}

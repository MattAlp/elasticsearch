/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.hamcrest.CoreMatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class AsyncSearchSingleNodeTests extends ESSingleNodeTestCase {
    private boolean resetTracingNode;

    @Override
    protected boolean resetNodeAfterTest() {
        return resetTracingNode;
    }

    public void testBackgroundSearchParentsShardWork() throws Exception {
        createIndex("traced");
        ensureGreen("traced");
        var exporter = InMemorySpanExporter.create();
        resetTracingNode = true;
        try (
            var sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
                .build()
        ) {
            var transportService = getInstanceFromNode(TransportService.class);
            transportService.getTaskManager().setOpenTelemetry(sdk);
            var threadContext = transportService.getThreadPool().getThreadContext();
            List<Span> parents = new ArrayList<>();
            try {
                var futures = new ArrayList<ActionFuture<AsyncSearchResponse>>();
                for (String name : List.of("first", "second")) {
                    var parent = sdk.getTracer("test").spanBuilder(name).setNoParent().startSpan();
                    parents.add(parent);
                    try (var scope = parent.makeCurrent()) {
                        var request = new SubmitAsyncSearchRequest(new SearchSourceBuilder().size(0));
                        request.getSearchRequest().indices("traced");
                        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
                        futures.add(client().execute(SubmitAsyncSearchAction.INSTANCE, request));
                        assertEquals(parent.getSpanContext(), Span.current().getSpanContext());
                        assertNull(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
                    }
                }
                for (var future : futures.reversed()) {
                    assertResponse(future, response -> {
                        assertFalse(response.isRunning());
                        assertNull(response.getFailure());
                    });
                }
                assertBusy(() -> {
                    var spans = exporter.getFinishedSpanItems();
                    for (var parent : parents) {
                        var trace = spans.stream().filter(span -> span.getTraceId().equals(parent.getSpanContext().getTraceId())).toList();
                        var submit = trace.stream()
                            .filter(span -> span.getName().equals(SubmitAsyncSearchAction.NAME))
                            .findFirst()
                            .orElseThrow();
                        var search = trace.stream()
                            .filter(span -> span.getName().equals(TransportSearchAction.TYPE.name()))
                            .findFirst()
                            .orElseThrow();
                        assertEquals(parent.getSpanContext().getSpanId(), submit.getParentSpanId());
                        assertEquals(submit.getSpanId(), search.getParentSpanId());
                        assertTrue(
                            "background search must parent shard work: " + trace,
                            trace.stream().anyMatch(span -> span.getParentSpanId().equals(search.getSpanId()))
                        );
                    }
                });
                assertFalse(Span.current().getSpanContext().isValid());
                assertNull(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER));
            } finally {
                parents.forEach(Span::end);
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(AsyncSearch.class, SubFetchPhasePlugin.class);
    }

    public void testFetchFailuresAllShards() throws Exception {
        for (int i = 0; i < 10; i++) {
            DocWriteResponse indexResponse = client().index(new IndexRequest("boom" + i).id("boom" + i).source("text", "value")).get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        client().admin().indices().refresh(new RefreshRequest()).get();

        TermsAggregationBuilder agg = new TermsAggregationBuilder("text").field("text.keyword");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().aggregation(agg);
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(sourceBuilder);
        submitAsyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(10));
        assertResponse(client().execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest), asyncSearchResponse -> {
            assertFalse(asyncSearchResponse.isRunning());
            assertTrue(asyncSearchResponse.isPartial());
            SearchResponse searchResponse = asyncSearchResponse.getSearchResponse();
            assertEquals(10, searchResponse.getTotalShards());
            assertEquals(10, searchResponse.getSuccessfulShards());
            assertEquals(0, searchResponse.getFailedShards());
            assertEquals(0, searchResponse.getShardFailures().length);
            assertEquals(10, searchResponse.getHits().getTotalHits().value());
            assertEquals(0, searchResponse.getHits().getHits().length);
            StringTerms terms = searchResponse.getAggregations().get("text");
            assertEquals(1, terms.getBuckets().size());
            assertEquals(10, terms.getBucketByKey("value").getDocCount());
            assertNotNull(asyncSearchResponse.getFailure());
            assertThat(asyncSearchResponse.getFailure(), CoreMatchers.instanceOf(ElasticsearchStatusException.class));
            ElasticsearchStatusException statusException = (ElasticsearchStatusException) asyncSearchResponse.getFailure();
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR, statusException.status());
            assertThat(asyncSearchResponse.getFailure().getCause(), CoreMatchers.instanceOf(SearchPhaseExecutionException.class));
            SearchPhaseExecutionException phaseExecutionException = (SearchPhaseExecutionException) asyncSearchResponse.getFailure()
                .getCause();
            assertEquals("fetch", phaseExecutionException.getPhaseName());
            assertEquals("boom", phaseExecutionException.getCause().getMessage());
            assertEquals(10, phaseExecutionException.shardFailures().length);
            for (ShardSearchFailure shardSearchFailure : phaseExecutionException.shardFailures()) {
                assertEquals("boom", shardSearchFailure.getCause().getMessage());
            }
        });
    }

    public void testFetchFailuresOnlySomeShards() throws Exception {
        for (int i = 0; i < 5; i++) {
            DocWriteResponse indexResponse = client().index(new IndexRequest("boom" + i).id("boom" + i).source("text", "value")).get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        for (int i = 0; i < 5; i++) {
            DocWriteResponse indexResponse = client().index(new IndexRequest("index" + i).id("index" + i).source("text", "value")).get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }
        client().admin().indices().refresh(new RefreshRequest()).get();

        TermsAggregationBuilder agg = new TermsAggregationBuilder("text").field("text.keyword");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().aggregation(agg);
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(sourceBuilder);
        submitAsyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(10));
        assertResponse(client().execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest), asyncSearchResponse -> {
            assertFalse(asyncSearchResponse.isRunning());
            assertFalse(asyncSearchResponse.isPartial());
            assertNull(asyncSearchResponse.getFailure());
            SearchResponse searchResponse = asyncSearchResponse.getSearchResponse();
            assertEquals(10, searchResponse.getTotalShards());
            assertEquals(5, searchResponse.getSuccessfulShards());
            assertEquals(5, searchResponse.getFailedShards());
            assertEquals(10, searchResponse.getHits().getTotalHits().value());
            assertEquals(5, searchResponse.getHits().getHits().length);
            StringTerms terms = searchResponse.getAggregations().get("text");
            assertEquals(1, terms.getBuckets().size());
            assertEquals(10, terms.getBucketByKey("value").getDocCount());
            assertEquals(5, searchResponse.getShardFailures().length);
            for (ShardSearchFailure shardFailure : searchResponse.getShardFailures()) {
                assertEquals("boom", shardFailure.getCause().getMessage());
            }
        });
    }

    public static final class SubFetchPhasePlugin extends Plugin implements SearchPlugin {
        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return Collections.singletonList(searchContext -> new FetchSubPhaseProcessor() {
                @Override
                public void setNextReader(LeafReaderContext readerContext) {}

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }

                @Override
                public void process(FetchSubPhase.HitContext hitContext) {
                    if (hitContext.hit().getId().startsWith("boom")) {
                        throw new RuntimeException("boom");
                    }

                }
            });
        }
    }
}

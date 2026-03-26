/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlHybridLateMaterializationSingleNodeIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(PlannerSettings.REDUCTION_LATE_MATERIALIZATION.getKey(), true)
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true)
                .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), 1)
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1)
                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
                .build()
        );
    }

    public void testFinalDriverLateMaterializesProjectionFieldAfterTwoStageScoring() throws Exception {
        String index = createAndPopulateBooksIndex();
        String query = """
            FROM %s METADATA _score, _id
            | WHERE author:"Tolkien"
            | SORT _score DESC, _id DESC
            | LIMIT 4
            | EVAL rerankish = id + 100
            | SORT rerankish DESC
            | LIMIT 2
            | KEEP id, title, rerankish
            """.formatted(index);

        try (var response = run(syncEsqlQueryRequest(query).pragmas(getPragmas()).profile(true))) {
            assertThat(response.isPartial(), equalTo(false));
            assertNotNull(response.profile());
            assertTrue("final should materialize title", driverReadsField(response, "final", "title"));
            assertFalse("data should not materialize title", driverReadsField(response, "data", "title"));
        }
    }

    public void testFuseSubplansLateMaterializeProjectionField() throws Exception {
        String index = createAndPopulateBooksIndex();
        String query = """
            FROM %s METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | KEEP _score, _id, title
            """.formatted(index);

        try (var response = run(syncEsqlQueryRequest(query).pragmas(getPragmas()).profile(true))) {
            assertThat(response.isPartial(), equalTo(false));
            assertNotNull(response.profile());
            assertTrue("first subplan final should materialize title", driverReadsField(response, "subplan-0.final", "title"));
            assertTrue("second subplan final should materialize title", driverReadsField(response, "subplan-1.final", "title"));
            assertFalse("data should not materialize title", driverReadsField(response, "data", "title"));
        }
    }

    public void testFinalDriverLateMaterializesSourceFieldAfterSecondLimit() throws Exception {
        String index = createAndPopulateBooksIndex();
        String query = """
            FROM %s
            | SORT id DESC
            | LIMIT 4
            | EVAL rerankish = id + 100
            | SORT rerankish DESC
            | LIMIT 2
            | STATS total = SUM(LENGTH(title))
            """.formatted(index);

        try (var response = run(syncEsqlQueryRequest(query).pragmas(getPragmas()).profile(true))) {
            assertThat(response.isPartial(), equalTo(false));
            assertNotNull(response.profile());
            assertThat("data should not materialize title", driverFieldValuesLoaded(response, "data", "title"), equalTo(0L));
            assertThat("final should only materialize the final two titles", driverFieldValuesLoaded(response, "final", "title"), equalTo(2L));
        }
    }

    private String createAndPopulateBooksIndex() {
        String index = "books-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("id", "type=integer", "author", "type=text", "title", "type=text")
        );
        client().prepareBulk()
            .add(new IndexRequest(index).id("1").source("id", 1, "author", "J.R.R. Tolkien", "title", "The Fellowship of the Ring"))
            .add(new IndexRequest(index).id("2").source("id", 2, "author", "J.R.R. Tolkien", "title", "The Two Towers"))
            .add(new IndexRequest(index).id("3").source("id", 3, "author", "J.R.R. Tolkien", "title", "The Return of the King"))
            .add(new IndexRequest(index).id("4").source("id", 4, "author", "William Faulkner", "title", "The Sound and the Fury"))
            .add(new IndexRequest(index).id("5").source("id", 5, "author", "William Faulkner", "title", "As I Lay Dying"))
            .add(new IndexRequest(index).id("6").source("id", 6, "author", "William Faulkner", "title", "Absalom, Absalom!"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(index);
        return index;
    }

    private static boolean driverReadsField(EsqlQueryResponse response, String driverName, String fieldName) {
        return driverFieldValuesLoaded(response, driverName, fieldName) > 0L;
    }

    private static long driverFieldValuesLoaded(EsqlQueryResponse response, String driverName, String fieldName) {
        return response.profile()
            .drivers()
            .stream()
            .filter(driverProfile -> driverProfile.description().equals(driverName))
            .flatMap(driverProfile -> driverProfile.operators().stream())
            .map(OperatorStatus::status)
            .filter(ValuesSourceReaderOperatorStatus.class::isInstance)
            .map(ValuesSourceReaderOperatorStatus.class::cast)
            .filter(status -> status.readersBuilt().keySet().stream().anyMatch(reader -> reader.startsWith(fieldName + ":")))
            .mapToLong(ValuesSourceReaderOperatorStatus::valuesLoaded)
            .sum();
    }
}

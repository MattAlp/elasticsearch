/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PlanRemoteFetchTests extends ESTestCase {

    public void testPlansBoundaryAndCoordinatorFetchInFinalOptimizerBatch() {
        Configuration configuration = configuration(true, true, MappedFieldType.FieldExtractPreference.NONE);
        PhysicalPlan optimized = distributedPlan(configuration, TransportVersion.current());

        assertThat(optimized.toString(), optimized.collect(RemoteFetchExec.class), hasSize(1));
        List<RemoteFetchBoundaryExec> boundaries = optimized.collect(RemoteFetchBoundaryExec.class);
        assertThat(boundaries, hasSize(1));
        RemoteFetchBoundaryExec boundary = boundaries.getFirst();
        assertThat(boundary.eagerAttributes().stream().map(Attribute::name).toList(), equalTo(List.of("hire_date")));
        assertThat(boundary.dataOutput().stream().map(Attribute::name).toList(), equalTo(List.of(MetadataNames.DOC, "hire_date")));
        assertThat(
            boundary.handoffOutput().stream().map(Attribute::name).toList(),
            equalTo(List.of(RemoteFetchHandle.ATTRIBUTE_NAME, "hire_date"))
        );

        RemoteFetchExec fetch = optimized.collect(RemoteFetchExec.class).getFirst();
        assertThat(fetch.attributesToFetch().stream().map(Attribute::name).toList(), containsInAnyOrder("salary", "emp_no"));
        assertThat(optimized.output().stream().map(Attribute::name).toList(), equalTo(List.of("hire_date", "salary", "emp_no")));

        var split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(optimized, configuration);
        assertThat(split.v1().collect(RemoteFetchExec.class), hasSize(1));
        ExchangeSinkExec dataPlan = as(split.v2(), ExchangeSinkExec.class);
        assertThat(dataPlan.output(), equalTo(boundary.handoffOutput()));
        assertThat(dataPlan.child(), instanceOf(RemoteFetchBoundaryExec.class));
    }

    public void testRemoteFetchReestimatesCoordinatorTopN() {
        PhysicalPlan original = distributedPlan(
            configuration(false, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current()
        );
        PhysicalPlan rewritten = distributedPlan(
            configuration(true, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current()
        );

        assertThat(
            rewritten.collect(org.elasticsearch.xpack.esql.plan.physical.TopNExec.class).getFirst().estimatedRowSize(),
            greaterThan(original.collect(org.elasticsearch.xpack.esql.plan.physical.TopNExec.class).getFirst().estimatedRowSize())
        );
    }

    public void testDisabledPragmaDisablesRemoteFetch() {
        PhysicalPlan optimized = distributedPlan(
            configuration(false, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current()
        );

        assertThat(optimized.toString(), optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testDisabledNodeLevelReductionDisablesRemoteFetch() {
        PhysicalPlan optimized = distributedPlan(
            configuration(true, false, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current()
        );

        assertThat(optimized.collect(RemoteFetchExec.class), hasSize(0));
        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testUnsupportedTransportVersionDisablesRemoteFetch() {
        TransportVersion unsupported = TransportVersionUtils.getPreviousVersion(RemoteFetchBoundaryExec.ESQL_REMOTE_FETCH_TOPN_REDUCTION);
        PhysicalPlan optimized = distributedPlan(configuration(true, true, MappedFieldType.FieldExtractPreference.NONE), unsupported);

        assertThat(optimized.collect(RemoteFetchExec.class), hasSize(0));
        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testNonDefaultExtractionPreferenceDisablesRemoteFetch() {
        PhysicalPlan optimized = distributedPlan(
            configuration(true, true, MappedFieldType.FieldExtractPreference.DOC_VALUES),
            TransportVersion.current()
        );

        assertThat(optimized.collect(RemoteFetchExec.class), hasSize(0));
        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testDoesNotPlanWithoutDeferredFields() {
        PhysicalPlan optimized = distributedPlan(
            configuration(true, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current(),
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date"
        );

        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testDoesNotPlanNestedPipelineBreaker() {
        PhysicalPlan optimized = distributedPlan(
            configuration(true, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current(),
            "FROM employees | SORT salary | LIMIT 100 | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no"
        );

        assertThat(optimized.toString(), optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testDoesNotRemoteFetchScore() {
        PhysicalPlan optimized = distributedPlan(
            configuration(true, true, MappedFieldType.FieldExtractPreference.NONE),
            TransportVersion.current(),
            "FROM employees METADATA _score | SORT hire_date | LIMIT 20 | KEEP hire_date, _score"
        );

        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    public void testDoesNotPlanSpecializedOrTemporalExtractionSemantics() {
        assertDeferredAttributeIsRejected(
            new FieldAttribute(Source.EMPTY, "specialized", new PotentiallyUnmappedKeywordEsField("specialized"))
        );
        assertDeferredAttributeIsRejected(
            new FieldAttribute(
                Source.EMPTY,
                "specialized",
                new MultiTypeEsField("specialized", DataType.DATE_NANOS, true, Map.of(), EsField.TimeSeriesFieldType.NONE, null)
            )
        );
        assertDeferredAttributeIsRejected(
            new FieldAttribute(
                Source.EMPTY,
                "location",
                new EsField("location", DataType.GEO_POINT, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );
        assertDeferredAttributeIsRejected(new TemporalityAttribute(Source.EMPTY));
    }

    private static PhysicalPlan distributedPlan(Configuration configuration, TransportVersion minimumVersion) {
        return distributedPlan(
            configuration,
            minimumVersion,
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no"
        );
    }

    private static PhysicalPlan distributedPlan(Configuration configuration, TransportVersion minimumVersion, String query) {
        Map<String, EsField> mapping = Map.of(
            "hire_date",
            new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "salary",
            new EsField("salary", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "emp_no",
            new EsField("emp_no", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Analyzer analyzer = EsqlTestUtils.analyzer()
            .addIndex(EsIndexGenerator.esIndex("employees", mapping, Map.of("employees", IndexMode.STANDARD)))
            .minimumTransportVersion(minimumVersion)
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query);
    }

    private static Configuration configuration(
        boolean remoteFetchTopN,
        boolean nodeLevelReduction,
        MappedFieldType.FieldExtractPreference preference
    ) {
        return EsqlTestUtils.configuration(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), remoteFetchTopN)
                    .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), nodeLevelReduction)
                    .put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), preference)
                    .build()
            )
        );
    }

    private static void assertDeferredAttributeIsRejected(Attribute deferredAttribute) {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute sort = new FieldAttribute(
            Source.EMPTY,
            "sort",
            new EsField("sort", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of("", List.of("test")),
            Map.of("", List.of("test")),
            Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of(doc, sort, deferredAttribute)
        );
        TopN topN = new TopN(Source.EMPTY, new Project(Source.EMPTY, relation, List.of(doc, sort)), order, EsqlTestUtils.of(10), false);
        Project dataProject = new Project(Source.EMPTY, topN, List.of(sort, deferredAttribute));
        ExchangeExec exchange = new ExchangeExec(Source.EMPTY, dataProject.output(), false, new FragmentExec(dataProject));
        PhysicalPlan plan = new ProjectExec(
            Source.EMPTY,
            new TopNExec(Source.EMPTY, exchange, order, EsqlTestUtils.of(10), 0),
            dataProject.output()
        );

        PhysicalPlan optimized = new PlanRemoteFetch().apply(
            plan,
            new PhysicalOptimizerContext(configuration(true, true, MappedFieldType.FieldExtractPreference.NONE), TransportVersion.current())
        );
        assertThat(optimized.collect(RemoteFetchBoundaryExec.class), hasSize(0));
    }

    private static <T> T as(Object value, Class<T> expectedType) {
        assertThat(value, instanceOf(expectedType));
        return expectedType.cast(value);
    }

    private static final class MetadataNames {
        private static final String DOC = "_doc";
    }
}

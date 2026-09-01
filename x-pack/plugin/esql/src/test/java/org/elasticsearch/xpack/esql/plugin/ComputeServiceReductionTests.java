/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.FetchHandleFunction;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies distributed fetch planning across the coordinator/data-node boundary, including compatibility gates and handoff schema.
 */
public class ComputeServiceReductionTests extends ESTestCase {
    public void testFetchBoundaryRequiresRuntimeBindings() {
        ExchangeSinkExec dataNodePlan = fetchDataNodePlan();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                dataNodePlan,
                true,
                true,
                null
            )
        );

        assertThat(e.getMessage(), containsString("fetch boundary requires local node and retained session identifiers"));
    }

    public void testFetchBoundaryRejectsMismatchedExchangeSchema() {
        ExchangeSinkExec dataNodePlan = fetchDataNodePlan();
        ExchangeSinkExec mismatched = new ExchangeSinkExec(
            dataNodePlan.source(),
            List.of(),
            dataNodePlan.isIntermediateAgg(),
            dataNodePlan.child()
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                mismatched,
                true,
                true,
                "node-a",
                "session-a[n]",
                null
            )
        );

        assertThat(e.getMessage(), containsString("does not match exchange output"));
    }

    public void testRejectsMultipleFetchBoundaries() {
        Attribute handle = new ReferenceAttribute(Source.EMPTY, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        PhysicalPlan source = new ExchangeSourceExec(Source.EMPTY, List.of(), false);
        FetchBoundaryExec inner = new FetchBoundaryExec(Source.EMPTY, source, handle, List.of(handle));
        FetchBoundaryExec outer = new FetchBoundaryExec(Source.EMPTY, inner, handle, List.of(handle));
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(Source.EMPTY, outer.output(), false, outer);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                dataNodePlan,
                true,
                true,
                "node-a",
                "session-a[n]",
                null
            )
        );

        assertThat(e.getMessage(), equalTo("expected at most one fetch boundary but found [2]"));
    }

    public void testFetchBoundaryRejectsNonTopNReduction() {
        Attribute handle = new ReferenceAttribute(Source.EMPTY, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        FragmentExec fragment = new FragmentExec(
            new Limit(Source.EMPTY, EsqlTestUtils.of(10), new LocalRelation(Source.EMPTY, List.of(handle), null))
        );
        FetchBoundaryExec boundary = new FetchBoundaryExec(Source.EMPTY, fragment, handle, List.of(handle));
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(Source.EMPTY, boundary.output(), false, boundary);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                dataNodePlan,
                true,
                true,
                "node-a",
                "session-a[n]",
                null
            )
        );

        assertThat(e.getMessage(), equalTo("fetch boundary must contain a Project -> TopN fragment"));
    }

    public void testFetchBoundaryMustBeDirectChildOfExchangeSink() {
        Attribute handle = new ReferenceAttribute(Source.EMPTY, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        FetchBoundaryExec boundary = new FetchBoundaryExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(), false),
            handle,
            List.of(handle)
        );
        ProjectExec project = new ProjectExec(Source.EMPTY, boundary, List.of(handle));
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(Source.EMPTY, project.output(), false, project);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                dataNodePlan,
                true,
                true,
                "node-a",
                "session-a[n]",
                null
            )
        );

        assertThat(e.getMessage(), equalTo("fetch boundary must be the direct child of the data-node exchange sink"));
    }

    public void testFetchBoundaryRequiresSupportedTopNFragmentShape() {
        Attribute handle = new ReferenceAttribute(Source.EMPTY, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        TopN topN = new TopN(Source.EMPTY, new LocalRelation(Source.EMPTY, List.of(handle), null), List.of(), EsqlTestUtils.of(10), false);
        FetchBoundaryExec boundary = new FetchBoundaryExec(Source.EMPTY, new FragmentExec(topN), handle, List.of(handle));
        ExchangeSinkExec dataNodePlan = new ExchangeSinkExec(Source.EMPTY, boundary.output(), false, boundary);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ComputeService.reductionPlan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                fetchConfiguration(),
                FoldContext.small(),
                dataNodePlan,
                true,
                true,
                "node-a",
                "session-a[n]",
                null
            )
        );

        assertThat(e.getMessage(), equalTo("fetch boundary must contain a Project -> TopN fragment"));
    }

    public void testPlansDistributedTopNBeforeCoordinatorDataNodeSplit() {
        PhysicalPlan distributedPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            EsqlTestUtils.TEST_CFG
        );
        TopNExec originalTopN = distributedPlan.collect(TopNExec.class).getFirst();
        ExchangeExec originalExchange = as(originalTopN.child(), ExchangeExec.class);
        FragmentExec originalFragment = as(originalExchange.child(), FragmentExec.class);

        PhysicalPlan planned = optimizeDeferredFetch(distributedPlan, fetchConfiguration(), TransportVersion.current());
        PhysicalPlan rewritten = planned;

        assertTrue(planned.anyMatch(FetchBoundaryExec.class::isInstance));
        ProjectExec project = as(rewritten, ProjectExec.class);
        FetchExec fetch = as(project.child(), FetchExec.class);
        TopNExec topN = as(fetch.child(), TopNExec.class);
        ExchangeExec exchange = as(topN.child(), ExchangeExec.class);
        assertThat(exchange.output().stream().map(Attribute::name).toList(), equalTo(List.of(FetchHandle.ATTRIBUTE_NAME, "hire_date")));
        assertThat(topN.inputOrdering(), equalTo(originalTopN.inputOrdering()));
        assertThat(topN.estimatedRowSize(), equalTo(originalTopN.estimatedRowSize()));
        FetchBoundaryExec fetchBoundary = as(exchange.child(), FetchBoundaryExec.class);
        assertThat(fetchBoundary.handleAttribute(), equalTo(exchange.output().getFirst()));
        assertThat(fetchBoundary.handoffOutput(), equalTo(exchange.output()));
        assertThat(as(fetchBoundary.child(), FragmentExec.class).estimatedRowSize(), equalTo(originalFragment.estimatedRowSize()));
        assertThat(fetchBoundary.child().output(), hasSize(2));
        assertTrue(EsQueryExec.isDocAttribute(fetchBoundary.child().output().getFirst()));
        assertThat(fetchBoundary.child().output().get(1).name(), equalTo("hire_date"));
        assertThat(rewritten.output(), hasSize(distributedPlan.output().size()));
        for (int i = 0; i < rewritten.output().size(); i++) {
            assertSame(distributedPlan.output().get(i), rewritten.output().get(i));
        }

        var split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(rewritten, EsqlTestUtils.TEST_CFG);
        ProjectExec coordinatorProject = as(split.v1(), ProjectExec.class);
        FetchExec coordinatorFetch = as(coordinatorProject.child(), FetchExec.class);
        TopNExec coordinatorTopN = as(coordinatorFetch.child(), TopNExec.class);
        ExchangeSourceExec coordinatorSource = as(coordinatorTopN.child(), ExchangeSourceExec.class);
        ExchangeSinkExec dataSink = as(split.v2(), ExchangeSinkExec.class);
        assertThat(dataSink.child(), instanceOf(FetchBoundaryExec.class));
        assertThat(coordinatorSource.output(), equalTo(dataSink.output()));
        for (int i = 0; i < coordinatorSource.output().size(); i++) {
            assertSame(coordinatorSource.output().get(i), dataSink.output().get(i));
        }
    }

    public void testPreservesDistributedOutputWithoutProject() {
        PhysicalPlan distributedPlan = distributedQueryPlan("FROM employees | SORT hire_date | LIMIT 20", EsqlTestUtils.TEST_CFG);
        assertThat(distributedPlan, instanceOf(TopNExec.class));

        PhysicalPlan rewritten = optimizeDeferredFetch(distributedPlan, fetchConfiguration(), TransportVersion.current());

        assertThat(rewritten, instanceOf(ProjectExec.class));
        assertThat(rewritten.output(), equalTo(distributedPlan.output()));
        assertFalse(rewritten.output().stream().anyMatch(FetchHandle::isAttribute));
    }

    public void testDoesNotPlanFetchForMultipleTopNs() {
        assertNoDeferredFetch("FROM employees | SORT hire_date | LIMIT 20 | SORT salary | LIMIT 10 | KEEP hire_date, salary, emp_no");
    }

    public void testDoesNotPlanFetchAcrossAnotherPipelineBreaker() {
        assertNoDeferredFetch("FROM employees | LIMIT 10 BY emp_no | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no");
    }

    public void testDoesNotPlanFetchForScore() {
        assertNoDeferredFetch("FROM employees METADATA _score | SORT hire_date | LIMIT 20 | KEEP hire_date, _score");
    }

    public void testFetchBoundaryRequiresTransportVersion() {
        Configuration configuration = EsqlTestUtils.configuration(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_TOPN.getKey(), true).build())
        );
        PhysicalPlan physicalPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            configuration
        );

        var planned = optimizeDeferredFetch(
            physicalPlan,
            configuration,
            TransportVersionUtils.getPreviousVersion(FetchBoundaryExec.ESQL_FETCH_BOUNDARY)
        );

        assertThat(planned.collect(FetchExec.class), hasSize(0));
        assertFalse(planned.anyMatch(FetchBoundaryExec.class::isInstance));
    }

    public void testDoesNotPlanFieldsWithSpecializedLoaderSemantics() {
        assertSpecializedAttributeIsNotFetchable(new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("pod")));
        assertSpecializedAttributeIsNotFetchable(new TemporalityAttribute(Source.EMPTY));
        assertSpecializedFieldIsNotFetchable(
            new FunctionEsField(
                new EsField("specialized", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                DataType.INTEGER,
                new BlockLoaderFunctionConfig.JustFunction(BlockLoaderFunctionConfig.Function.LENGTH)
            )
        );
        assertSpecializedFieldIsNotFetchable(new PotentiallyUnmappedKeywordEsField("specialized"));
        assertSpecializedFieldIsNotFetchable(
            new MultiTypeEsField("specialized", DataType.DATE_NANOS, true, Map.of(), EsField.TimeSeriesFieldType.NONE, null)
        );
    }

    public void testDoesNotPlanSpatialFieldsWithConfiguredLoaderSemantics() {
        Configuration configuration = EsqlTestUtils.configuration(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.FETCH_TOPN.getKey(), true)
                    .put(QueryPragmas.FIELD_EXTRACT_PREFERENCE.getKey(), MappedFieldType.FieldExtractPreference.DOC_VALUES.name())
                    .build()
            )
        );

        for (DataType dataType : DataType.values()) {
            if (DataType.isSpatial(dataType)) {
                assertSpecializedAttributeIsNotFetchable(field("spatial", dataType), configuration);
            }
        }
    }

    public void testFetchReductionConsumesBoundaryProjectWithoutDuplicatingDoc() {
        ExchangeSinkExec dataNodePlan = fetchDataNodePlan();
        FetchBoundaryExec fetchBoundary = as(dataNodePlan.child(), FetchBoundaryExec.class);
        Attribute doc = fetchBoundary.child().output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElseThrow();

        ReductionPlan reductionPlan = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            fetchConfiguration(),
            FoldContext.small(),
            dataNodePlan,
            true,
            true,
            "node-a",
            "session-a[n]",
            null
        );

        assertDocAttributes(as(reductionPlan.dataNodePlan().child(), FragmentExec.class).fragment(), doc, 1);
        assertThat(reductionPlan.dataNodePlan().output(), equalTo(fetchBoundary.child().output()));
    }

    public void testCoordinatorAndReducePlansUseFetchHandleSchema() {
        PhysicalPlan distributedPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            EsqlTestUtils.TEST_CFG
        );
        PhysicalPlan planned = optimizeDeferredFetch(distributedPlan, fetchConfiguration(), TransportVersion.current());
        var splitPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(planned, EsqlTestUtils.TEST_CFG);
        PhysicalPlan coordinatorPlan = splitPlan.v1();
        ExchangeSinkExec dataNodePlan = as(splitPlan.v2(), ExchangeSinkExec.class);
        FetchBoundaryExec fetchBoundary = as(dataNodePlan.child(), FetchBoundaryExec.class);
        Attribute doc = fetchBoundary.child().output().getFirst();
        Attribute hireDate = fetchBoundary.child().output().get(1);

        /*
         * coordinator: Project[hire_date, salary, emp_no]
         *                  \- Fetch[salary, emp_no]
         *                       |- TopN[hire_date] -> ExchangeSource[handle, hire_date]
         * data: ExchangeSink[handle, hire_date]
         *           \- Fragment[Project[doc, hire_date] -> TopN[hire_date] -> EsRelation]
         */
        assertThat(dataNodePlan.output().getFirst().name(), equalTo(FetchHandle.ATTRIBUTE_NAME));
        assertThat(dataNodePlan.output(), equalTo(List.of(dataNodePlan.output().getFirst(), hireDate)));
        assertThat(fetchBoundary.child().output(), equalTo(List.of(doc, hireDate)));

        ProjectExec rewrittenProject = as(coordinatorPlan, ProjectExec.class);
        FetchExec fetch = as(rewrittenProject.child(), FetchExec.class);
        Attribute salary = fetch.attributesToFetch().getFirst();
        Attribute empNo = fetch.attributesToFetch().get(1);
        assertThat(fetch.attributesToFetch(), equalTo(List.of(salary, empNo)));
        assertThat(fetch.child(), instanceOf(TopNExec.class));

        ReductionPlan reductionPlan = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            fetchConfiguration(),
            FoldContext.small(),
            dataNodePlan,
            true,
            true,
            "node-a",
            "session-a[n]",
            null
        );

        /*
         * shard data: ExchangeSink[doc, hire_date]
         *                 \- Fragment[Project[doc, hire_date] -> TopN[hire_date] -> EsRelation]
         * node reduce: ExchangeSink[handle, hire_date]
         *                  \- Project[handle, hire_date]
         *                       \- Eval[handle] -> TopN[hire_date] -> ExchangeSource[doc, hire_date]
         */
        assertThat(reductionPlan.dataNodePlan().output(), equalTo(List.of(doc, hireDate)));
        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(dataNodePlan.output()));
        assertThat(reductionPlan.dataNodePlan().collect(FetchBoundaryExec.class), hasSize(0));
        assertThat(reductionPlan.nodeReducePlan().collect(FetchBoundaryExec.class), hasSize(0));

        ProjectExec handleProject = as(reductionPlan.nodeReducePlan().child(), ProjectExec.class);
        EvalExec handleEval = as(handleProject.child(), EvalExec.class);
        TopNExec reductionTopN = handleEval.child().collect(TopNExec.class).getFirst();
        assertThat(reductionTopN.inputOrdering(), equalTo(TopNOperator.InputOrdering.SORTED));
        Alias handleAlias = handleEval.fields().getFirst();
        Attribute plannedHandle = dataNodePlan.output().getFirst();
        assertTrue(plannedHandle.synthetic());
        assertThat(handleAlias.toAttribute().name(), equalTo(plannedHandle.name()));
        assertThat(handleAlias.toAttribute().dataType(), equalTo(plannedHandle.dataType()));
        assertThat(handleAlias.toAttribute().id(), equalTo(plannedHandle.id()));
        assertThat(handleAlias.child(), instanceOf(FetchHandleFunction.class));
    }

    public void testFetchBoundaryWithoutNodeReductionStillBuildsHandleHandoff() {
        PhysicalPlan distributedPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            EsqlTestUtils.TEST_CFG
        );
        PhysicalPlan planned = optimizeDeferredFetch(distributedPlan, fetchConfiguration(), TransportVersion.current());
        var splitPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(planned, EsqlTestUtils.TEST_CFG);
        PhysicalPlan coordinatorPlan = splitPlan.v1();
        ExchangeSinkExec dataNodePlan = as(splitPlan.v2(), ExchangeSinkExec.class);
        FetchBoundaryExec fetchBoundary = as(dataNodePlan.child(), FetchBoundaryExec.class);
        Attribute doc = fetchBoundary.child().output().getFirst();
        Attribute hireDate = fetchBoundary.child().output().get(1);

        ReductionPlan reductionPlan = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            fetchConfiguration(),
            FoldContext.small(),
            dataNodePlan,
            false,
            true,
            "node-a",
            "session-a[n]",
            null
        );

        // The shard and coordinator retain their TopNs, but the node-reduce stage only converts docs to fetch handles.
        assertThat(as(reductionPlan.dataNodePlan().child(), FragmentExec.class).fragment().collect(TopN.class), hasSize(1));
        assertThat(coordinatorPlan.collect(TopNExec.class), hasSize(1));
        assertThat(reductionPlan.nodeReducePlan().collect(TopNExec.class), hasSize(0));
        assertThat(reductionPlan.dataNodePlan().output(), equalTo(List.of(doc, hireDate)));
        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(dataNodePlan.output()));

        ProjectExec handleProject = as(reductionPlan.nodeReducePlan().child(), ProjectExec.class);
        EvalExec handleEval = as(handleProject.child(), EvalExec.class);
        ExchangeSourceExec exchangeSource = as(handleEval.child(), ExchangeSourceExec.class);
        assertThat(exchangeSource.output(), equalTo(fetchBoundary.child().output()));
        assertThat(handleProject.output(), equalTo(fetchBoundary.handoffOutput()));
        Alias handleAlias = handleEval.fields().getFirst();
        assertThat(handleAlias.toAttribute().name(), equalTo(fetchBoundary.handleAttribute().name()));
        assertThat(handleAlias.toAttribute().dataType(), equalTo(fetchBoundary.handleAttribute().dataType()));
        assertThat(handleAlias.toAttribute().id(), equalTo(fetchBoundary.handleAttribute().id()));
        assertThat(handleAlias.child(), instanceOf(FetchHandleFunction.class));
    }

    public void testUserColumnNamedLikeFetchHandleIsNotTreatedAsInternalHandle() {
        Attribute userColumn = new ReferenceAttribute(Source.EMPTY, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(userColumn),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(userColumn), false)
        );

        ReductionPlan reductionPlan = ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            EsqlTestUtils.configuration(new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_TOPN.getKey(), true).build())),
            FoldContext.small(),
            plan,
            false,
            false,
            null
        );

        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(plan.output()));
        assertThat(plan.collect(FetchBoundaryExec.class), hasSize(0));
    }

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static void assertSpecializedFieldIsNotFetchable(EsField specializedField) {
        assertSpecializedAttributeIsNotFetchable(new FieldAttribute(Source.EMPTY, "specialized", specializedField));
    }

    private static void assertSpecializedAttributeIsNotFetchable(Attribute specialized) {
        assertSpecializedAttributeIsNotFetchable(specialized, fetchConfiguration());
    }

    private static void assertSpecializedAttributeIsNotFetchable(Attribute specialized, Configuration configuration) {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        Attribute sort = field("sort", DataType.LONG);
        List<Order> order = List.of(new Order(Source.EMPTY, sort, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(LOCAL_CLUSTER_GROUP_KEY, List.of("test")),
            Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of(doc, sort, specialized)
        );
        Project dataProject = new Project(
            Source.EMPTY,
            new TopN(Source.EMPTY, new Project(Source.EMPTY, relation, List.of(doc, sort)), order, EsqlTestUtils.of(10), false),
            List.of(sort, specialized)
        );
        PhysicalPlan distributedPlan = new ProjectExec(
            Source.EMPTY,
            new TopNExec(
                Source.EMPTY,
                new ExchangeExec(Source.EMPTY, dataProject.output(), false, new FragmentExec(dataProject)),
                order,
                EsqlTestUtils.of(10),
                0
            ),
            dataProject.output()
        );

        PhysicalPlan planned = optimizeDeferredFetch(distributedPlan, configuration, TransportVersion.current());
        assertThat(planned.collect(FetchExec.class), hasSize(0));
        assertFalse(planned.anyMatch(FetchBoundaryExec.class::isInstance));
    }

    private static void assertNoDeferredFetch(String query) {
        PhysicalPlan physicalPlan = distributedQueryPlan(query, EsqlTestUtils.TEST_CFG);
        PhysicalPlan planned = optimizeDeferredFetch(physicalPlan, fetchConfiguration(), TransportVersion.current());
        assertThat(planned.collect(FetchExec.class), hasSize(0));
        assertFalse(planned.anyMatch(FetchBoundaryExec.class::isInstance));
    }

    private static PhysicalPlan optimizeDeferredFetch(
        PhysicalPlan distributedPlan,
        Configuration configuration,
        TransportVersion minimumTransportVersion
    ) {
        PhysicalPlan resolvedPlan = distributedPlan.transformDown(
            FragmentExec.class,
            fragment -> fragment.withFragment(fragment.fragment().transformDown(EsRelation.class, relation -> {
                if (relation.concreteIndices().isEmpty() == false) {
                    return relation;
                }
                List<String> concreteIndices = relation.indexProperties().isEmpty()
                    ? List.of(relation.indexPattern())
                    : List.copyOf(relation.indexProperties().keySet());
                return new EsRelation(
                    relation.source(),
                    relation.indexPattern(),
                    relation.indexMode(),
                    relation.originalIndices(),
                    Map.of(LOCAL_CLUSTER_GROUP_KEY, concreteIndices),
                    relation.indexProperties(),
                    relation.output()
                );
            }))
        );
        return new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, minimumTransportVersion)).optimize(resolvedPlan);
    }

    private static Configuration fetchConfiguration() {
        return EsqlTestUtils.configuration(new QueryPragmas(Settings.builder().put(QueryPragmas.FETCH_TOPN.getKey(), true).build()));
    }

    private static ExchangeSinkExec fetchDataNodePlan() {
        PhysicalPlan distributedPlan = distributedQueryPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no",
            EsqlTestUtils.TEST_CFG
        );
        PhysicalPlan planned = optimizeDeferredFetch(distributedPlan, fetchConfiguration(), TransportVersion.current());
        return as(PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(planned, EsqlTestUtils.TEST_CFG).v2(), ExchangeSinkExec.class);
    }

    private static PhysicalPlan distributedQueryPlan(String query, Configuration configuration) {
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
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).mappedPlan(query);
    }

    private static <T> T as(Object value, Class<T> expectedType) {
        assertThat(value, instanceOf(expectedType));
        return expectedType.cast(value);
    }

    private static void assertDocAttributes(LogicalPlan plan, Attribute expectedDoc, int count) {
        EsRelation relation = plan.collect(EsRelation.class).getFirst();
        List<Attribute> docs = relation.output().stream().filter(EsQueryExec::isDocAttribute).toList();
        assertThat(docs, hasSize(count));
        docs.forEach(doc -> assertSame(expectedDoc, doc));
    }
}

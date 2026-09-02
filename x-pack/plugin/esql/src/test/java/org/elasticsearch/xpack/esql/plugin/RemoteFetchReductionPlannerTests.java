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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class RemoteFetchReductionPlannerTests extends ESTestCase {

    public void testRemoteFetchHandleUsesSyntheticAttributeName() {
        assertThat(RemoteFetchHandle.ATTRIBUTE_NAME, startsWith(Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX));
    }

    public void testConsumesOptimizerBoundaryForNodeReduction() {
        Configuration configuration = remoteFetchConfiguration();
        PhysicalPlan distributedPlan = distributedQueryPlan(configuration);
        var split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(distributedPlan, configuration);
        assertThat(split.v1().collect(RemoteFetchExec.class), hasSize(1));
        ExchangeSinkExec dataPlan = as(split.v2(), ExchangeSinkExec.class);
        RemoteFetchBoundaryExec boundary = as(dataPlan.child(), RemoteFetchBoundaryExec.class);

        assertThat(boundary.eagerAttributes().stream().map(Attribute::name).toList(), equalTo(List.of("hire_date")));
        assertThat(
            split.v1().collect(RemoteFetchExec.class).getFirst().attributesToFetch().stream().map(Attribute::name).toList(),
            containsInAnyOrder("salary", "emp_no")
        );

        ReductionPlan reductionPlan = ReductionPlanner.plan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            FoldContext.small(),
            dataPlan,
            true,
            true,
            new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]"),
            null
        );

        assertThat(reductionPlan.dataNodePlan().output(), equalTo(boundary.dataOutput()));
        assertThat(reductionPlan.nodeReducePlan().output(), equalTo(boundary.handoffOutput()));
        ProjectExec handleProject = as(reductionPlan.nodeReducePlan().child(), ProjectExec.class);
        EvalExec handleEval = as(handleProject.child(), EvalExec.class);
        TopNExec reductionTopN = handleEval.child().collect(TopNExec.class).getFirst();
        assertThat(reductionTopN.inputOrdering(), equalTo(TopNOperator.InputOrdering.SORTED));
        Alias handleAlias = handleEval.fields().getFirst();
        assertThat(handleAlias.toAttribute().id(), equalTo(boundary.handleAttribute().id()));
        assertThat(handleAlias.child(), instanceOf(RemoteFetchHandleFunction.class));
    }

    public void testUserColumnNamedLikeRemoteFetchHandleIsNotTreatedAsInternalHandle() {
        Attribute userColumn = new ReferenceAttribute(Source.EMPTY, null, RemoteFetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD);
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(userColumn),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(userColumn), false)
        );

        assertFalse(RemoteFetchHandle.isRemoteFetchHandleCarrier(userColumn));
        assertTrue(
            RemoteFetchReductionPlanner.planReduceDriverTopN(
                stats -> new org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext(
                    PlannerSettings.DEFAULTS,
                    new EsqlFlags(false),
                    EsqlTestUtils.TEST_CFG,
                    FoldContext.small(),
                    stats
                ),
                plan,
                new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]")
            ).isEmpty()
        );
    }

    public void testFailsWhenCoordinatorCommittedWithoutBoundary() {
        Attribute handle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
        ExchangeSinkExec sink = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(handle),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(handle), false)
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ReductionPlanner.plan(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(false),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                sink,
                true,
                true,
                new RemoteFetchReductionPlanner.RemoteFetchContext("node-a", "session-a[n]"),
                null
            )
        );
        assertThat(e.getMessage(), containsString("node reduction could not be rebuilt"));
        assertThat(e.getMessage(), not(containsString(sink.toString())));
    }

    private static PhysicalPlan distributedQueryPlan(Configuration configuration) {
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
            .minimumTransportVersion(TransportVersion.current())
            .buildAnalyzer();
        return new TestPlannerOptimizer(configuration, analyzer).distributedPlan(
            "FROM employees | SORT hire_date | LIMIT 20 | KEEP hire_date, salary, emp_no"
        );
    }

    private static Configuration remoteFetchConfiguration() {
        return EsqlTestUtils.configuration(new QueryPragmas(Settings.builder().put(QueryPragmas.REMOTE_FETCH_TOPN.getKey(), true).build()));
    }

    private static <T> T as(Object value, Class<T> expectedType) {
        assertThat(value, instanceOf(expectedType));
        return expectedType.cast(value);
    }
}

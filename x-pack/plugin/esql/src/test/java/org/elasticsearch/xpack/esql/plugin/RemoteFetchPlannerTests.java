/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTests;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EmitRemoteFetchHandleExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.CsvTests.loadIndexResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteFetchPlannerTests extends ESTestCase {
    public void testCoordinatorPlanIsRewrittenToRemoteFetch() {
        PlannedQuery planned = planQuery("""
            FROM employees
            | KEEP hire_date, salary
            | SORT hire_date
            | LIMIT 5
            """);
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            planned.physicalPlan(),
            planned.configuration()
        );

        PhysicalPlan coordinatorPlan = RemoteFetchPlanner.planCoordinatorTopN(
            contextFactory(planned.configuration()),
            split.v1(),
            (ExchangeSinkExec) split.v2()
        ).orElseThrow();

        ProjectExec projectExec = (ProjectExec) coordinatorPlan;
        assertThat(projectExec.child(), instanceOf(RemoteFetchExec.class));

        RemoteFetchExec remoteFetchExec = (RemoteFetchExec) projectExec.child();
        assertThat(remoteFetchExec.attributesToFetch().stream().map(Attribute::name).toList(), contains("salary"));

        ExchangeSourceExec exchangeSource = singleValue(remoteFetchExec.child().collect(ExchangeSourceExec.class));
        assertTrue(
            exchangeSource.output().stream().anyMatch(attribute -> RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME.equals(attribute.name()))
        );
        assertFalse(exchangeSource.output().stream().anyMatch(EsQueryExec::isDocAttribute));
    }

    public void testReductionPlanEmitsRemoteFetchHandles() {
        PlannedQuery planned = planQuery("""
            FROM employees
            | KEEP hire_date, salary
            | SORT hire_date
            | LIMIT 5
            """);
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            planned.physicalPlan(),
            planned.configuration()
        );

        ReductionPlan reductionPlan = RemoteFetchPlanner.planReduceDriverTopN(
            contextFactory(planned.configuration()),
            (ExchangeSinkExec) split.v2()
        ).orElseThrow();

        assertThat(reductionPlan.nodeReducePlan().child(), instanceOf(EmitRemoteFetchHandleExec.class));
        EmitRemoteFetchHandleExec emitRemoteFetchHandleExec = (EmitRemoteFetchHandleExec) reductionPlan.nodeReducePlan().child();
        assertTrue(EsQueryExec.isDocAttribute(emitRemoteFetchHandleExec.sourceAttribute()));
        assertEquals(RemoteFetchPlanner.REMOTE_FETCH_HANDLE_NAME, emitRemoteFetchHandleExec.handleAttribute().name());
        assertTrue(reductionPlan.dataNodePlan().output().stream().anyMatch(EsQueryExec::isDocAttribute));
        assertFalse(reductionPlan.nodeReducePlan().output().stream().anyMatch(EsQueryExec::isDocAttribute));
    }

    private PlannedQuery planQuery(String query) {
        TransportVersion transportVersion = TransportVersion.current();
        EsqlStatement statement = TEST_PARSER.createStatement(query);
        LogicalPlan parsedPlan = statement.plan();
        TestAnalyzer testAnalyzer = analyzer().addLanguagesLookup()
            .addAnalysisTestsEnrichResolution()
            .addAnalysisTestsInferenceResolution()
            .minimumTransportVersion(transportVersion)
            .unmappedResolution(statement.setting(QuerySettings.UNMAPPED_FIELDS));
        loadIndexResolution(CsvTests.testDatasets(parsedPlan)).forEach(
            (pattern, resolution) -> testAnalyzer.addIndex(pattern.indexPattern(), resolution)
        );
        Analyzer analyzer = testAnalyzer.buildAnalyzer();
        Configuration configuration = analyzer.context().configuration();
        LogicalPlan logicalPlan = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(configuration, configuration.newFoldContext(), transportVersion)
        ).optimize(analyzer.analyze(parsedPlan));
        PhysicalPlan physicalPlan = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, transportVersion)).optimize(
            new Mapper().map(new Versioned<>(logicalPlan, transportVersion))
        );
        return new PlannedQuery(configuration, physicalPlan);
    }

    private static Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory(Configuration configuration) {
        return stats -> new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            configuration,
            configuration.newFoldContext(),
            stats
        );
    }

    private static <T> T singleValue(List<T> values) {
        assertEquals(1, values.size());
        return values.get(0);
    }

    private record PlannedQuery(Configuration configuration, PhysicalPlan physicalPlan) {}
}

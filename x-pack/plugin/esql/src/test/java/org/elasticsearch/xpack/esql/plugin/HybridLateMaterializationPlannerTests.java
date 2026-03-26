/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class HybridLateMaterializationPlannerTests extends AbstractLocalPhysicalPlanOptimizerTests {

    public HybridLateMaterializationPlannerTests(String name, Configuration config) {
        super(name, config);
    }

    public void testEvalBetweenTopNsLateMaterializesScoringField() {
        ReductionPlan reductionPlan = reductionPlan(
            """
                FROM test
                | KEEP emp_no, salary
                | SORT emp_no
                | LIMIT 20
                | EVAL my_score = salary + 1
                | SORT my_score
                | LIMIT 5
                """,
            makeAnalyzer("mapping-basic.json")
        );

        assertThat(reductionPlan.localPhysicalOptimization(), equalTo(LocalPhysicalOptimization.DISABLED));
        assertThat(fieldExtracts(reductionPlan.dataNodePlan()), empty());

        Set<String> dataOutput = attributeNames(reductionPlan.dataNodePlan().output());
        assertThat(dataOutput, equalTo(Set.of("_doc", "emp_no")));

        List<FieldExtractExec> reductionExtracts = fieldExtracts(reductionPlan.nodeReducePlan());
        assertThat(reductionExtracts, hasSize(1));
        assertThat(attributeNames(reductionExtracts.getFirst().attributesToExtract()), equalTo(Set.of("salary")));
    }

    public void testFuseSubplansLateMaterializeBranchFields() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan(
            """
                FROM books METADATA _id, _index, _score
                | FORK
                  ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
                  ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
                | FUSE
                | SORT _score DESC, _id, _index
                | KEEP _score, _id, title
                """,
            analyzer
        );

        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        assertThat(subplansAndMainPlan.v1(), hasSize(2));

        for (PhysicalPlan subplan : subplansAndMainPlan.v1()) {
            Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
            assertNotNull("expected a data-node fragment inside the subplan", coordinatorAndDataPlan.v2());

            ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());

            assertThat(reductionPlan.localPhysicalOptimization(), equalTo(LocalPhysicalOptimization.DISABLED));
            assertThat(fieldExtracts(reductionPlan.dataNodePlan()), empty());

            Set<String> dataOutput = attributeNames(reductionPlan.dataNodePlan().output());
            assertThat(dataOutput, hasItem("_doc"));
            assertThat(dataOutput, hasItem("_score"));
            assertThat(dataOutput, hasItem("_id"));
            assertThat(dataOutput, not(hasItem("title")));

            List<FieldExtractExec> reductionExtracts = fieldExtracts(reductionPlan.nodeReducePlan());
            assertThat(reductionExtracts, hasSize(1));
            assertThat(attributeNames(reductionExtracts.getFirst().attributesToExtract()), hasItem("title"));
        }
    }

    private Analyzer booksAnalyzer() {
        return makeAnalyzer(
            IndexResolution.valid(
                EsIndexGenerator.esIndex(
                    "books",
                    EsqlTestUtils.loadMapping("mapping-books.json"),
                    Map.of("books", IndexMode.STANDARD)
                )
            )
        );
    }

    private ReductionPlan reductionPlan(String query, Analyzer analyzer) {
        PhysicalPlan plan = physicalPlan(query, analyzer);
        Tuple<PhysicalPlan, PhysicalPlan> plans = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(plan, config);
        assertNotNull("expected a data-node plan", plans.v2());
        return reductionPlan((ExchangeSinkExec) plans.v2());
    }

    private ReductionPlan reductionPlan(ExchangeSinkExec dataNodePlan) {
        return ComputeService.reductionPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            config,
            FoldContext.small(),
            dataNodePlan,
            true,
            true,
            new PlanTimeProfile()
        );
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        LogicalPlan logical = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(config, FoldContext.small(), analyzer.context().minimumVersion())
        ).optimize(analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query)));
        return new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config, analyzer.context().minimumVersion())).optimize(
            new Mapper().map(new Versioned<>(logical, analyzer.context().minimumVersion()))
        );
    }

    private static List<FieldExtractExec> fieldExtracts(PhysicalPlan plan) {
        return plan.collect(node -> node instanceof FieldExtractExec).stream().map(FieldExtractExec.class::cast).toList();
    }

    private static Set<String> attributeNames(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).collect(Collectors.toSet());
    }
}

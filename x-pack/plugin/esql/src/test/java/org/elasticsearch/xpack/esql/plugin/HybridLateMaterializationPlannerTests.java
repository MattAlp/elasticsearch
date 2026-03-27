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
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Materialize;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeMode;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeTarget;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.RerankExec;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class HybridLateMaterializationPlannerTests extends AbstractLocalPhysicalPlanOptimizerTests {

    public HybridLateMaterializationPlannerTests(String name, Configuration config) {
        super(name, config);
    }

    public void testEvalBetweenTopNsLateMaterializesScoringField() {
        ReductionPlan reductionPlan = reductionPlan("""
            FROM test
            | KEEP emp_no, salary
            | SORT emp_no
            | LIMIT 20
            | EVAL my_score = salary + 1
            | SORT my_score
            | LIMIT 5
            """, makeAnalyzer("mapping-basic.json"));

        assertThat(reductionPlan.localPhysicalOptimization(), equalTo(LocalPhysicalOptimization.DISABLED));
        assertThat(fieldExtracts(reductionPlan.dataNodePlan()), empty());

        Set<String> dataOutput = attributeNames(reductionPlan.dataNodePlan().output());
        assertThat(dataOutput, equalTo(Set.of("_doc", "emp_no")));

        List<FieldExtractExec> reductionExtracts = fieldExtracts(reductionPlan.nodeReducePlan());
        assertThat(reductionExtracts, hasSize(1));
        assertThat(attributeNames(reductionExtracts.getFirst().attributesToExtract()), equalTo(Set.of("salary")));
        List<MaterializeExec> materializeExecs = materializeExecs(reductionPlan.nodeReducePlan());
        assertThat(materializeExecs, hasSize(1));
        assertThat(attributeNames(materializeExecs.getFirst().deferredAttributes()), equalTo(Set.of("salary")));
        assertThat(materializeExecs.getFirst().target(), equalTo(MaterializeTarget.CURRENT_FINAL));
        assertThat(lateFieldAttributes(reductionPlan).keySet(), equalTo(Set.of("salary")));
    }

    public void testInsertMaterializeBoundaryForTopNFragment() {
        PhysicalPlan plan = physicalPlan("""
            FROM test
            | KEEP emp_no, salary
            | SORT emp_no
            | LIMIT 20
            | EVAL my_score = salary + 1
            | SORT my_score
            | LIMIT 5
            """, makeAnalyzer("mapping-basic.json"));
        Tuple<PhysicalPlan, PhysicalPlan> plans = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(plan, config);
        ExchangeSinkExec dataNodePlan = (ExchangeSinkExec) plans.v2();
        FragmentExec fragmentExec = (FragmentExec) dataNodePlan.child();

        LateMaterializationPlanner.MaterializeBoundaryFragment boundary = LateMaterializationPlanner.materializeBoundaryFragment(
            stats -> new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, new EsqlFlags(false), config, FoldContext.small(), stats),
            fragmentExec.fragment(),
            MaterializeTarget.CURRENT_FINAL
        ).orElseThrow();
        LogicalPlan materializedFragment = boundary.project();
        assertThat(materializedFragment, instanceOf(Project.class));

        TopN topN = (TopN) ((Project) materializedFragment).child();
        assertThat(topN.child(), instanceOf(Materialize.class));

        Materialize materialize = (Materialize) topN.child();
        assertThat(attributeNames(materialize.output()), equalTo(Set.of("_doc", "emp_no", "salary")));
        assertThat(attributeNames(materialize.carryAttributes()), equalTo(Set.of("emp_no")));
        assertThat(attributeNames(materialize.deferredAttributes()), equalTo(Set.of("salary")));
        assertThat(materialize.target(), equalTo(MaterializeTarget.CURRENT_FINAL));
        assertThat(materialize.mode(), equalTo(MaterializeMode.LOCAL));
        assertThat(attributeNames(boundary.passthroughOutput()), equalTo(Set.of("_doc", "emp_no")));
    }

    public void testParentFinalReductionFragmentUsesMaterializeBoundaryMetadata() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(author))
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ExchangeSinkExec dataNodePlan = (ExchangeSinkExec) coordinatorAndDataPlan.v2();
        ReductionPlan reductionPlan = reductionPlan(dataNodePlan);
        Map<String, FieldAttribute> lateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            dataNodePlan,
            reductionPlan
        );
        FragmentExec fragmentExec = (FragmentExec) dataNodePlan.child();

        LateMaterializationPlanner.MaterializeBoundaryFragment boundary = LateMaterializationPlanner.materializeBoundaryFragment(
            fragmentExec.fragment(),
            lateFields.values().stream().sorted(java.util.Comparator.comparing(Attribute::name)).map(Attribute.class::cast).toList(),
            MaterializeTarget.PARENT_FINAL
        ).orElseThrow();
        LogicalPlan materializedFragment = boundary.project();
        assertThat(materializedFragment, instanceOf(Project.class));

        TopN topN = (TopN) ((Project) materializedFragment).child();
        assertThat(topN.child(), instanceOf(Materialize.class));
        Materialize materialize = boundary.materialize();
        assertThat(attributeNames(materialize.carryAttributes()), equalTo(Set.of("_score", "_id", "_index")));
        assertThat(attributeNames(materialize.deferredAttributes()), equalTo(Set.of("title", "author")));
        assertThat(materialize.target(), equalTo(MaterializeTarget.PARENT_FINAL));
        assertThat(materialize.mode(), equalTo(MaterializeMode.LOCAL));
        assertThat(attributeNames(boundary.passthroughOutput()), equalTo(Set.of("_doc", "_score", "_id", "_index")));

        ExchangeSinkExec rewrittenDataPlan = dataNodePlanWithParentFinalBoundary(dataNodePlan, lateFields);
        assertThat(attributeNames(rewrittenDataPlan.output()), equalTo(Set.of("_doc", "_score", "_id", "_index")));
        FragmentExec rewrittenFragment = (FragmentExec) rewrittenDataPlan.child();
        Project rewrittenProject = (Project) rewrittenFragment.fragment();
        assertThat(
            rewrittenProject.projections().stream().map(NamedExpression::name).collect(Collectors.toSet()),
            equalTo(Set.of("_doc", "_score", "_id", "_index"))
        );
    }

    public void testSupportedParentFinalReductionShapesAllExposeMaterializeBoundary() {
        assertParentFinalReductionBoundary("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(title))
            """, Set.of("title"));
        assertParentFinalReductionBoundary("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(author))
            """, Set.of("title", "author"));
        assertParentFinalReductionBoundary("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | WHERE LENGTH(author) > 3
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | KEEP _score, _id, title
            """, Set.of("author", "title"));
    }

    public void testNonTopNReductionPlanStaysLocallyOptimizable() {
        PhysicalPlan plan = physicalPlan("""
            FROM books
            | STATS total = COUNT(*)
            """, booksAnalyzer());
        Tuple<PhysicalPlan, PhysicalPlan> plans = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(plan, config);
        assertThat(plans.v2(), instanceOf(ExchangeSinkExec.class));

        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) plans.v2());
        assertThat(reductionPlan.localPhysicalOptimization(), equalTo(LocalPhysicalOptimization.ENABLED));
        assertThat(materializeExecs(reductionPlan.nodeReducePlan()), empty());
    }

    public void testFuseSubplansLateMaterializeBranchFields() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _id, _index, _score
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | KEEP _score, _id, title
            """, analyzer);

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

    public void testFuseMainPlanLateMaterializesFieldAfterGlobalLimit() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(title))
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Attribute docAttribute = reductionPlan.dataNodePlan()
            .output()
            .stream()
            .filter(EsQueryExec::isDocAttribute)
            .findFirst()
            .orElseThrow();

        PhysicalPlan rewrittenMainPlan = mainPlanWithDocPassthrough(
            subplansAndMainPlan.v2(),
            docAttribute,
            lateFieldAttributes((ExchangeSinkExec) coordinatorAndDataPlan.v2(), reductionPlan)
        );
        PhysicalPlan optimizedMainPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenMainPlan,
            new PlanTimeProfile()
        );

        AggregateExec fuseAggregate = optimizedMainPlan.collect(
            node -> node instanceof AggregateExec aggregate && aggregate.child() instanceof FuseScoreEvalExec
        ).stream().map(AggregateExec.class::cast).findFirst().orElseThrow();
        assertThat(attributeNames(fuseAggregate.output()), hasItem("_doc"));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("title")));

        List<MaterializeExec> materializeExecs = materializeExecs(optimizedMainPlan);
        assertThat(materializeExecs, hasSize(1));
        assertThat(attributeNames(materializeExecs.getFirst().deferredAttributes()), equalTo(Set.of("title")));
        assertThat(materializeExecs.getFirst().target(), equalTo(MaterializeTarget.CURRENT_FINAL));

        List<FieldExtractExec> extracts = fieldExtracts(optimizedMainPlan);
        assertThat(extracts, hasSize(1));
        assertThat(attributeNames(extracts.getFirst().attributesToExtract()), equalTo(Set.of("title")));
        assertThat(extracts.getFirst().child(), instanceOf(TopNExec.class));
    }

    public void testFuseMainPlanRerankLateMaterializesFieldAfterFuseLimit() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | KEEP _score, _id, title
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Attribute docAttribute = reductionPlan.dataNodePlan()
            .output()
            .stream()
            .filter(EsQueryExec::isDocAttribute)
            .findFirst()
            .orElseThrow();

        Map<String, FieldAttribute> mainLateFields = new java.util.HashMap<>(lateFieldAttributes(subplansAndMainPlan.v2()));
        mainLateFields.putAll(lateFieldAttributes(subplan));
        mainLateFields.putAll(lateFieldAttributes((ExchangeSinkExec) coordinatorAndDataPlan.v2(), reductionPlan));
        assertThat(mainLateFields.keySet(), equalTo(Set.of("title")));

        PhysicalPlan rewrittenMainPlan = mainPlanWithDocPassthrough(subplansAndMainPlan.v2(), docAttribute, mainLateFields);
        PhysicalPlan optimizedMainPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenMainPlan,
            new PlanTimeProfile()
        );

        AggregateExec fuseAggregate = optimizedMainPlan.collect(
            node -> node instanceof AggregateExec aggregate && aggregate.child() instanceof FuseScoreEvalExec
        ).stream().map(AggregateExec.class::cast).findFirst().orElseThrow();
        assertThat(attributeNames(fuseAggregate.output()), hasItem("_doc"));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("title")));

        RerankExec rerank = optimizedMainPlan.collect(node -> node instanceof RerankExec)
            .stream()
            .map(RerankExec.class::cast)
            .findFirst()
            .orElseThrow();
        assertThat(rerank.child(), instanceOf(FieldExtractExec.class));
        FieldExtractExec rerankExtract = (FieldExtractExec) rerank.child();
        assertThat(attributeNames(rerankExtract.attributesToExtract()), equalTo(Set.of("title")));

        List<FieldExtractExec> extracts = fieldExtracts(optimizedMainPlan);
        assertThat(extracts, hasSize(1));
        assertThat(attributeNames(extracts.getFirst().attributesToExtract()), equalTo(Set.of("title")));
        assertThat(extracts.getFirst().child(), instanceOf(TopNExec.class));
    }

    public void testFuseMainPlanRerankLateMaterializesSecondFieldAfterPostRerankLimit() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(author))
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Attribute docAttribute = reductionPlan.dataNodePlan()
            .output()
            .stream()
            .filter(EsQueryExec::isDocAttribute)
            .findFirst()
            .orElseThrow();

        Map<String, FieldAttribute> mainLateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            (ExchangeSinkExec) coordinatorAndDataPlan.v2(),
            reductionPlan
        );
        assertThat(mainLateFields.keySet(), equalTo(Set.of("title", "author")));

        PhysicalPlan rewrittenMainPlan = mainPlanWithDocPassthrough(subplansAndMainPlan.v2(), docAttribute, mainLateFields);
        PhysicalPlan optimizedMainPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenMainPlan,
            new PlanTimeProfile()
        );

        AggregateExec fuseAggregate = optimizedMainPlan.collect(
            node -> node instanceof AggregateExec aggregate && aggregate.child() instanceof FuseScoreEvalExec
        ).stream().map(AggregateExec.class::cast).findFirst().orElseThrow();
        assertThat(attributeNames(fuseAggregate.output()), hasItem("_doc"));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("title")));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("author")));

        List<MaterializeExec> materializeExecs = materializeExecs(optimizedMainPlan);
        assertThat(materializeExecs, hasSize(1));
        assertThat(attributeNames(materializeExecs.getFirst().deferredAttributes()), equalTo(Set.of("title", "author")));
        assertThat(materializeExecs.getFirst().target(), equalTo(MaterializeTarget.CURRENT_FINAL));

        RerankExec rerank = optimizedMainPlan.collect(node -> node instanceof RerankExec)
            .stream()
            .map(RerankExec.class::cast)
            .findFirst()
            .orElseThrow();
        assertThat(rerank.child(), instanceOf(FieldExtractExec.class));
        FieldExtractExec rerankExtract = (FieldExtractExec) rerank.child();
        assertThat(attributeNames(rerankExtract.attributesToExtract()), equalTo(Set.of("title")));
        assertThat(rerankExtract.child(), instanceOf(TopNExec.class));

        List<FieldExtractExec> extracts = fieldExtracts(optimizedMainPlan);
        assertThat(extracts, hasSize(2));
        assertThat(
            extracts.stream().map(extract -> attributeNames(extract.attributesToExtract())).collect(Collectors.toSet()),
            equalTo(Set.of(Set.of("title"), Set.of("author")))
        );
        FieldExtractExec authorExtract = extracts.stream()
            .filter(extract -> attributeNames(extract.attributesToExtract()).equals(Set.of("author")))
            .findFirst()
            .orElseThrow();
        assertThat(authorExtract.child(), instanceOf(TopNExec.class));
        assertTrue("author extract should stay above rerank", authorExtract.child().anyMatch(RerankExec.class::isInstance));
    }

    public void testParentFinalCoordinatorBoundaryLowersDeferredFieldsLocally() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | STATS total = SUM(LENGTH(author))
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Map<String, FieldAttribute> lateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            (ExchangeSinkExec) coordinatorAndDataPlan.v2(),
            reductionPlan
        );

        PhysicalPlan rewrittenCoordinatorPlan = coordinatorPlanWithParentFinalBoundary(
            coordinatorAndDataPlan.v1(),
            reductionPlan,
            lateFields
        );
        List<MaterializeExec> beforeLowering = materializeExecs(rewrittenCoordinatorPlan);
        assertThat(beforeLowering, hasSize(1));
        assertThat(attributeNames(beforeLowering.getFirst().deferredAttributes()), equalTo(Set.of("title", "author")));
        assertThat(beforeLowering.getFirst().target(), equalTo(MaterializeTarget.PARENT_FINAL));
        assertThat(attributeNames(beforeLowering.getFirst().child().output()), hasItem("title"));
        assertThat(attributeNames(beforeLowering.getFirst().child().output()), hasItem("author"));

        PhysicalPlan optimizedCoordinatorPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenCoordinatorPlan,
            new PlanTimeProfile()
        );
        List<MaterializeExec> afterLowering = materializeExecs(optimizedCoordinatorPlan);
        assertThat(afterLowering, hasSize(1));
        assertThat(attributeNames(afterLowering.getFirst().deferredAttributes()), equalTo(Set.of("title", "author")));
        assertThat(afterLowering.getFirst().target(), equalTo(MaterializeTarget.PARENT_FINAL));
        assertThat(attributeNames(afterLowering.getFirst().child().output()), not(hasItem("title")));
        assertThat(attributeNames(afterLowering.getFirst().child().output()), not(hasItem("author")));
        assertThat(fieldExtracts(optimizedCoordinatorPlan), empty());
    }

    public void testFuseMainPlanRerankScoreFilterPrunesBeforeSecondFetch() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | RERANK "Tolkien" ON title WITH { "inference_id" : "reranking-inference-id" }
            | WHERE _score > 3.5
            | SORT _score DESC, _id, _index
            | LIMIT 1
            | KEEP _score, _id, author
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Attribute docAttribute = reductionPlan.dataNodePlan()
            .output()
            .stream()
            .filter(EsQueryExec::isDocAttribute)
            .findFirst()
            .orElseThrow();

        Map<String, FieldAttribute> mainLateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            (ExchangeSinkExec) coordinatorAndDataPlan.v2(),
            reductionPlan
        );
        assertThat(mainLateFields.keySet(), equalTo(Set.of("title", "author")));

        PhysicalPlan rewrittenMainPlan = mainPlanWithDocPassthrough(subplansAndMainPlan.v2(), docAttribute, mainLateFields);
        PhysicalPlan optimizedMainPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenMainPlan,
            new PlanTimeProfile()
        );

        AggregateExec fuseAggregate = optimizedMainPlan.collect(
            node -> node instanceof AggregateExec aggregate && aggregate.child() instanceof FuseScoreEvalExec
        ).stream().map(AggregateExec.class::cast).findFirst().orElseThrow();
        assertThat(attributeNames(fuseAggregate.output()), hasItem("_doc"));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("title")));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("author")));

        RerankExec rerank = optimizedMainPlan.collect(node -> node instanceof RerankExec)
            .stream()
            .map(RerankExec.class::cast)
            .findFirst()
            .orElseThrow();
        assertThat(rerank.child(), instanceOf(FieldExtractExec.class));
        FieldExtractExec rerankExtract = (FieldExtractExec) rerank.child();
        assertThat(attributeNames(rerankExtract.attributesToExtract()), equalTo(Set.of("title")));
        assertThat(rerankExtract.child(), instanceOf(TopNExec.class));

        List<FieldExtractExec> extracts = fieldExtracts(optimizedMainPlan);
        assertThat(extracts, hasSize(2));
        assertThat(
            extracts.stream().map(extract -> attributeNames(extract.attributesToExtract())).collect(Collectors.toSet()),
            equalTo(Set.of(Set.of("title"), Set.of("author")))
        );
        FieldExtractExec authorExtract = extracts.stream()
            .filter(extract -> attributeNames(extract.attributesToExtract()).equals(Set.of("author")))
            .findFirst()
            .orElseThrow();
        assertThat(authorExtract.child(), instanceOf(TopNExec.class));
        assertTrue("author extract should stay above the score filter", authorExtract.child().anyMatch(FilterExec.class::isInstance));
        assertTrue("author extract should stay above rerank", authorExtract.child().anyMatch(RerankExec.class::isInstance));
    }

    public void testFuseMainPlanFilterLateMaterializesDifferentFieldsAtTwoUseSites() {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan("""
            FROM books METADATA _score, _id, _index
            | FORK
              ( WHERE author:"Tolkien" | SORT _score DESC, _id DESC | LIMIT 3 )
              ( WHERE author:"Faulkner" | SORT _score DESC, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | LIMIT 4
            | WHERE LENGTH(author) > 3
            | SORT _score DESC, _id, _index
            | LIMIT 2
            | KEEP _score, _id, title
            """, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ReductionPlan reductionPlan = reductionPlan((ExchangeSinkExec) coordinatorAndDataPlan.v2());
        Attribute docAttribute = reductionPlan.dataNodePlan()
            .output()
            .stream()
            .filter(EsQueryExec::isDocAttribute)
            .findFirst()
            .orElseThrow();

        Map<String, FieldAttribute> mainLateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            (ExchangeSinkExec) coordinatorAndDataPlan.v2(),
            reductionPlan
        );
        assertThat(mainLateFields.keySet(), equalTo(Set.of("author", "title")));

        PhysicalPlan rewrittenMainPlan = mainPlanWithDocPassthrough(subplansAndMainPlan.v2(), docAttribute, mainLateFields);
        PhysicalPlan optimizedMainPlan = PlannerUtils.localCoordinatorPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            List.of(),
            config,
            FoldContext.small(),
            rewrittenMainPlan,
            new PlanTimeProfile()
        );

        AggregateExec fuseAggregate = optimizedMainPlan.collect(
            node -> node instanceof AggregateExec aggregate && aggregate.child() instanceof FuseScoreEvalExec
        ).stream().map(AggregateExec.class::cast).findFirst().orElseThrow();
        assertThat(attributeNames(fuseAggregate.output()), hasItem("_doc"));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("author")));
        assertThat(attributeNames(fuseAggregate.output()), not(hasItem("title")));

        FilterExec filter = optimizedMainPlan.collect(node -> node instanceof FilterExec exec && exec.child() instanceof FieldExtractExec)
            .stream()
            .map(FilterExec.class::cast)
            .findFirst()
            .orElseThrow();
        FieldExtractExec filterExtract = (FieldExtractExec) filter.child();
        assertThat(attributeNames(filterExtract.attributesToExtract()), equalTo(Set.of("author")));
        assertThat(filterExtract.child(), instanceOf(TopNExec.class));

        List<FieldExtractExec> extracts = fieldExtracts(optimizedMainPlan);
        assertThat(extracts, hasSize(2));
        assertThat(
            extracts.stream().map(extract -> attributeNames(extract.attributesToExtract())).collect(Collectors.toSet()),
            equalTo(Set.of(Set.of("author"), Set.of("title")))
        );
        FieldExtractExec titleExtract = extracts.stream()
            .filter(extract -> attributeNames(extract.attributesToExtract()).equals(Set.of("title")))
            .findFirst()
            .orElseThrow();
        assertThat(titleExtract.child(), instanceOf(TopNExec.class));
        assertTrue("title extract should stay above the late filter", titleExtract.child().anyMatch(FilterExec.class::isInstance));
    }

    private Analyzer booksAnalyzer() {
        IndexResolution indexResolution = IndexResolution.valid(
            EsIndexGenerator.esIndex("books", EsqlTestUtils.loadMapping("mapping-books.json"), Map.of("books", IndexMode.STANDARD))
        );
        return AnalyzerTestUtils.analyzer(
            AnalyzerTestUtils.indexResolutions(indexResolution),
            AnalyzerTestUtils.defaultLookupResolution(),
            new EnrichResolution(),
            EsqlTestUtils.TEST_VERIFIER,
            config
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

    private static List<MaterializeExec> materializeExecs(PhysicalPlan plan) {
        return plan.collect(MaterializeExec.class::isInstance).stream().map(MaterializeExec.class::cast).toList();
    }

    private static Set<String> attributeNames(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).collect(Collectors.toSet());
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ReductionPlan reductionPlan) {
        Map<String, FieldAttribute> materializedLateFieldAttributes = lateFieldAttributesFromMaterializeBoundary(
            reductionPlan.nodeReducePlan()
        );
        return materializedLateFieldAttributes.isEmpty()
            ? lateFieldAttributes(reductionPlan.nodeReducePlan())
            : materializedLateFieldAttributes;
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(PhysicalPlan plan) {
        return plan.collect(node -> node instanceof FieldExtractExec)
            .stream()
            .map(FieldExtractExec.class::cast)
            .flatMap(fieldExtract -> fieldExtract.attributesToExtract().stream())
            .filter(FieldAttribute.class::isInstance)
            .map(FieldAttribute.class::cast)
            .collect(Collectors.toMap(FieldAttribute::name, field -> field, (left, right) -> left));
    }

    private static Map<String, FieldAttribute> lateFieldAttributesFromMaterializeBoundary(PhysicalPlan plan) {
        return plan.collect(MaterializeExec.class::isInstance)
            .stream()
            .map(MaterializeExec.class::cast)
            .flatMap(materialize -> materialize.deferredAttributes().stream())
            .filter(FieldAttribute.class::isInstance)
            .map(FieldAttribute.class::cast)
            .collect(Collectors.toMap(FieldAttribute::name, field -> field, (left, right) -> left));
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ExchangeSinkExec originalDataNodePlan, ReductionPlan reductionPlan) {
        Map<String, FieldAttribute> lateFieldAttributes = new java.util.HashMap<>(lateFieldAttributes(reductionPlan));
        Set<String> passthroughNames = attributeNames(reductionPlan.dataNodePlan().output());
        PhysicalPlan dataNodeChild = originalDataNodePlan.child();
        if (dataNodeChild instanceof org.elasticsearch.xpack.esql.plan.physical.FragmentExec fragmentExec
            && fragmentExec.fragment() instanceof org.elasticsearch.xpack.esql.plan.logical.Project project) {
            Map<String, FieldAttribute> fragmentFieldAttributes = new java.util.HashMap<>();
            fragmentExec.fragment()
                .forEachExpressionDown(FieldAttribute.class, field -> fragmentFieldAttributes.putIfAbsent(field.name(), field));
            fragmentExec.fragment()
                .collect(org.elasticsearch.xpack.esql.plan.logical.EsRelation.class)
                .forEach(
                    relation -> relation.output()
                        .stream()
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .forEach(field -> fragmentFieldAttributes.putIfAbsent(field.name(), field))
                );
            for (var projection : project.projections()) {
                if (passthroughNames.contains(projection.name())) {
                    continue;
                }
                FieldAttribute fieldAttribute = null;
                if (projection instanceof FieldAttribute field) {
                    fieldAttribute = field;
                } else if (projection instanceof Alias alias && alias.child() instanceof FieldAttribute field) {
                    fieldAttribute = (FieldAttribute) field.withName(alias.name()).withId(alias.id());
                }
                if (fieldAttribute == null) {
                    fieldAttribute = fragmentFieldAttributes.get(projection.name());
                }
                if (fieldAttribute != null) {
                    lateFieldAttributes.putIfAbsent(projection.name(), fieldAttribute);
                }
            }
        }
        return lateFieldAttributes;
    }

    private static Map<String, FieldAttribute> parentFinalLateFieldAttributes(
        PhysicalPlan mainPlan,
        PhysicalPlan subplan,
        ExchangeSinkExec originalDataNodePlan,
        ReductionPlan reductionPlan
    ) {
        Map<String, FieldAttribute> lateFieldAttributes = new java.util.HashMap<>(lateFieldAttributes(subplan));
        lateFieldAttributes.putAll(lateFieldAttributes(originalDataNodePlan, reductionPlan));
        lateFieldAttributes.putAll(lateFieldAttributes(originalDataNodePlan, mainPlan));
        lateFieldAttributes.putAll(lateFieldAttributes(mainPlan));
        return lateFieldAttributes;
    }

    private static Map<String, FieldAttribute> lateFieldAttributes(ExchangeSinkExec originalDataNodePlan, PhysicalPlan parentPlan) {
        Set<String> parentAttributeNames = new java.util.HashSet<>();
        parentPlan.forEachExpressionDown(Attribute.class, attribute -> parentAttributeNames.add(attribute.name()));
        Map<String, FieldAttribute> lateFieldAttributes = new java.util.HashMap<>();
        PhysicalPlan dataNodeChild = originalDataNodePlan.child();
        if (dataNodeChild instanceof org.elasticsearch.xpack.esql.plan.physical.FragmentExec fragmentExec
            && fragmentExec.fragment() instanceof org.elasticsearch.xpack.esql.plan.logical.Project project) {
            Map<String, FieldAttribute> fragmentFieldAttributes = new java.util.HashMap<>();
            fragmentExec.fragment()
                .forEachExpressionDown(FieldAttribute.class, field -> fragmentFieldAttributes.putIfAbsent(field.name(), field));
            fragmentExec.fragment()
                .collect(org.elasticsearch.xpack.esql.plan.logical.EsRelation.class)
                .forEach(
                    relation -> relation.output()
                        .stream()
                        .filter(FieldAttribute.class::isInstance)
                        .map(FieldAttribute.class::cast)
                        .forEach(field -> fragmentFieldAttributes.putIfAbsent(field.name(), field))
                );
            for (var projection : project.projections()) {
                if (parentAttributeNames.contains(projection.name()) == false) {
                    continue;
                }
                FieldAttribute fieldAttribute = null;
                if (projection instanceof FieldAttribute field) {
                    fieldAttribute = field;
                } else if (projection instanceof Alias alias && alias.child() instanceof FieldAttribute field) {
                    fieldAttribute = (FieldAttribute) field.withName(alias.name()).withId(alias.id());
                }
                if (fieldAttribute == null) {
                    fieldAttribute = fragmentFieldAttributes.get(projection.name());
                }
                if (fieldAttribute != null) {
                    lateFieldAttributes.putIfAbsent(projection.name(), fieldAttribute);
                }
            }
        }
        return lateFieldAttributes;
    }

    private static PhysicalPlan mainPlanWithDocPassthrough(
        PhysicalPlan mainPlan,
        Attribute docAttribute,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        return mainPlan.transformExpressionsDown(Expression.class, expression -> {
            if (expression instanceof Attribute attribute) {
                FieldAttribute lateField = lateFieldAttributes.get(attribute.name());
                if (lateField != null) {
                    return lateField.withId(attribute.id());
                }
            }
            return expression;
        }).transformUp(ExchangeSourceExec.class, exchangeSource -> {
            ExchangeSourceExec exchangeSourceWithDoc = exchangeSourceWithDoc(exchangeSource, docAttribute);
            List<Attribute> deferredAttributes = lateFieldAttributes.values()
                .stream()
                .sorted(java.util.Comparator.comparing(Attribute::name))
                .map(Attribute.class::cast)
                .toList();
            Set<String> deferredNames = deferredAttributes.stream().map(Attribute::name).collect(Collectors.toSet());
            List<Attribute> carryAttributes = exchangeSourceWithDoc.output()
                .stream()
                .filter(attr -> EsQueryExec.isDocAttribute(attr) == false)
                .filter(attr -> deferredNames.contains(attr.name()) == false)
                .toList();
            return new MaterializeExec(
                exchangeSource.source(),
                exchangeSourceWithDoc,
                docAttribute,
                carryAttributes,
                deferredAttributes,
                MaterializeTarget.CURRENT_FINAL,
                MaterializeMode.LOCAL
            );
        });
    }

    private static PhysicalPlan coordinatorPlanWithParentFinalBoundary(
        PhysicalPlan coordinatorPlan,
        ReductionPlan reductionPlan,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        Attribute docAttribute = reductionPlan.dataNodePlan().output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElseThrow();
        var replaced = new java.util.concurrent.atomic.AtomicBoolean();
        PhysicalPlan updatedPlan = coordinatorPlan.transformUp(ExchangeSourceExec.class, exchangeSource -> {
            if (replaced.compareAndSet(false, true)) {
                ExchangeSourceExec exchangeSourceWithDoc = exchangeSourceWithDoc(exchangeSource, docAttribute);
                List<Attribute> deferredAttributes = lateFieldAttributes.values()
                    .stream()
                    .sorted(java.util.Comparator.comparing(Attribute::name))
                    .map(Attribute.class::cast)
                    .toList();
                Set<String> deferredNames = deferredAttributes.stream().map(Attribute::name).collect(Collectors.toSet());
                List<Attribute> carryAttributes = exchangeSourceWithDoc.output()
                    .stream()
                    .filter(attr -> EsQueryExec.isDocAttribute(attr) == false)
                    .filter(attr -> deferredNames.contains(attr.name()) == false)
                    .toList();
                return new MaterializeExec(
                    exchangeSource.source(),
                    exchangeSourceWithDoc,
                    docAttribute,
                    carryAttributes,
                    deferredAttributes,
                    MaterializeTarget.PARENT_FINAL,
                    MaterializeMode.LOCAL
                );
            }
            return exchangeSource;
        });
        if (updatedPlan instanceof ExchangeSinkExec sink) {
            PhysicalPlan sinkChild = sink.child();
            if (sinkChild instanceof ProjectExec project) {
                sinkChild = project.child();
            }
            List<Attribute> sinkOutput = sinkChild.output().stream().filter(attr -> lateFieldAttributes.containsKey(attr.name()) == false).toList();
            updatedPlan = new ExchangeSinkExec(sink.source(), sinkOutput, sink.isIntermediateAgg(), sinkChild);
        }
        return updatedPlan;
    }

    private static ExchangeSinkExec dataNodePlanWithParentFinalBoundary(ExchangeSinkExec dataNodePlan, Map<String, FieldAttribute> lateFieldAttributes) {
        FragmentExec fragmentExec = (FragmentExec) dataNodePlan.child();
        Project project = (Project) fragmentExec.fragment();
        LateMaterializationPlanner.MaterializeBoundaryFragment boundary = parentFinalReductionBoundary(dataNodePlan, lateFieldAttributes).orElseThrow();
        FragmentExec updatedFragment = fragmentExec.withFragment(
            new Project(project.source(), boundary.loweredTopN(), boundary.passthroughProjections(project.projections()))
        );
        return dataNodePlan.replaceChildAndUpdateOutput(updatedFragment);
    }

    private void assertParentFinalReductionBoundary(String query, Set<String> expectedDeferred) {
        Analyzer analyzer = booksAnalyzer();
        PhysicalPlan plan = physicalPlan(query, analyzer);
        Tuple<List<PhysicalPlan>, PhysicalPlan> subplansAndMainPlan = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        PhysicalPlan subplan = subplansAndMainPlan.v1().getFirst();
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataPlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(subplan, config);
        ExchangeSinkExec dataNodePlan = (ExchangeSinkExec) coordinatorAndDataPlan.v2();
        ReductionPlan reductionPlan = reductionPlan(dataNodePlan);
        Map<String, FieldAttribute> lateFields = parentFinalLateFieldAttributes(
            subplansAndMainPlan.v2(),
            subplan,
            dataNodePlan,
            reductionPlan
        );
        LateMaterializationPlanner.MaterializeBoundaryFragment boundary = parentFinalReductionBoundary(dataNodePlan, lateFields).orElseThrow();
        assertThat(attributeNames(boundary.materialize().deferredAttributes()), equalTo(expectedDeferred));
        assertThat(boundary.materialize().target(), equalTo(MaterializeTarget.PARENT_FINAL));
        assertThat(attributeNames(boundary.passthroughOutput()), equalTo(Set.of("_doc", "_score", "_id", "_index")));

        ExchangeSinkExec rewrittenDataPlan = dataNodePlanWithParentFinalBoundary(dataNodePlan, lateFields);
        assertThat(attributeNames(rewrittenDataPlan.output()), equalTo(attributeNames(boundary.passthroughOutput())));
    }

    private static java.util.Optional<LateMaterializationPlanner.MaterializeBoundaryFragment> parentFinalReductionBoundary(
        ExchangeSinkExec dataNodePlan,
        Map<String, FieldAttribute> lateFieldAttributes
    ) {
        FragmentExec fragmentExec = (FragmentExec) dataNodePlan.child();
        Project project = (Project) fragmentExec.fragment();
        return LateMaterializationPlanner.materializeBoundaryFragment(
            project,
            lateFieldAttributes.values().stream().sorted(java.util.Comparator.comparing(Attribute::name)).map(Attribute.class::cast).toList(),
            MaterializeTarget.PARENT_FINAL
        );
    }

    private static ExchangeSourceExec exchangeSourceWithDoc(ExchangeSourceExec exchangeSource, Attribute docAttribute) {
        if (exchangeSource.output().stream().anyMatch(EsQueryExec::isDocAttribute)) {
            return exchangeSource;
        }
        List<Attribute> output = new java.util.ArrayList<>(exchangeSource.output().size() + 1);
        output.add(docAttribute);
        output.addAll(exchangeSource.output());
        return new ExchangeSourceExec(exchangeSource.source(), output, exchangeSource.isIntermediateAgg());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LowerLocalMaterialize;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Materialize;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeMode;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeTarget;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
* Modify a {@link Project} that follows a {@link TopN} such that it tries to minimize field extraction on the data driver.
*
* Consider the following query:
* <pre>
* FROM index | WHERE x > 10 | SORT foo | LIMIT 10 | KEEP bar
* </pre>
* If we can delay materializing {@code bar} until the node-reduce driver has finished its own TopN, we can reduce the amount of data we
* read from the index.
*
* The basic strategy here is to "cut off" the operation right after the last top n, and perform all the removed operations on the
* node reduce drivier, so the data drivers top n operations "feed into" the node reduce one. Ideally, we would just take the top-most
* {@link TopNExec}, but unfortunately that doesn't quite work: the top n might be pushed down to the source in {@link PushTopNToSource},
* which might change the output attributes (the filter might also be pushed down, so no {@code x} will be output). To solve this, we add a
* {@link Project} to ensure that the output schema of the data-side plan remains consistent with the expectations of the reduce-side
* plan (note that while performing the reduce-side plan we have no way of knowing if a pushdown is possible or not, since we don't have
* access to the source's capabilities).
*
* So for the aforementioned query, we would go from (roughly) this plan:
* <pre>
*  Project [bar]
*  └── TopN [foo, limit=10] (this will output _doc, foo, and x)
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  Into this:
*  <pre>
*  Project [_doc, foo, x]
*  └── TopN [foo, limit=10]
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  Now even if there's a pushdown, the <i>final</i> plan would be:
*  <pre>
*  Project [_doc, foo, x]
*  └── EsQuery [index with some TopN pushdown]
*  </pre>
*  The above actually reads the {@code x} field "unnecessarily", since it's only needed to conform to the output schema of the original
*  plan. See #134363 for a way to optimize this little problem.
*/
class LateMaterializationPlanner {
    public static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        FragmentExec fragmentExec = originalPlan.child() instanceof FragmentExec fe ? fe : null;
        if (fragmentExec == null) {
            return Optional.empty();
        }

        Project topLevelProject = fragmentExec.fragment() instanceof Project p ? p : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }

        TopN topN = topLevelProject.child() instanceof TopN tn ? tn : null;
        if (topN == null) { // I'm getting go déjà vu
            return Optional.empty();
        }

        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        PhysicalPlan mappedReductionPlan = toPhysical(fragmentExec.fragment(), context);
        List<Attribute> deferredAttributes = lateMaterializeAttributes(mappedReductionPlan);

        MaterializeBoundaryFragment boundary = materializeBoundaryFragment(
            fragmentExec.fragment(),
            deferredAttributes,
            MaterializeTarget.CURRENT_FINAL
        ).orElse(null);
        LogicalPlan reductionFragment = boundary == null ? fragmentExec.fragment() : boundary.project();
        Project reductionProject = reductionFragment instanceof Project p ? p : null;
        if (reductionProject == null || (reductionProject.child() instanceof TopN) == false) {
            return Optional.empty();
        }
        topLevelProject = reductionProject;
        topN = (TopN) reductionProject.child();
        Materialize materialize = topN.child() instanceof Materialize m ? m : null;
        if (boundary != null) {
            mappedReductionPlan = toPhysical(reductionFragment, context);
            deferredAttributes = boundary.materialize().deferredAttributes();
        }

        Attribute doc;
        List<Attribute> expectedDataOutput;
        LogicalPlan dataFragmentChild;
        if (boundary != null) {
            doc = boundary.materialize().rowIdentity();
            expectedDataOutput = boundary.passthroughOutput();
            dataFragmentChild = boundary.loweredTopN();
        } else {
            List<Attribute> physicalPlanOutput = toPhysical(topN, context).output();
            doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
            if (doc == null) {
                return Optional.empty();
            }

            LogicalPlan withAddedDocToRelation = topN.transformUp(EsRelation.class, r -> {
                if (r.indexMode() == IndexMode.LOOKUP) {
                    return r;
                }
                List<Attribute> attributes = CollectionUtils.prependToCopy(doc, r.output());
                return r.withAttributes(attributes);
            });
            if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
                // Defensive check: if any intermediate projects (or possibly another operator) removed the doc field, just abort this
                // optimization altogether!
                return Optional.empty();
            }

            AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(o -> o.references().stream()).toList());
            // Get the output from the physical plan below the TopN, and filter it to only the attributes needed for the final output
            // (either
            // because they are in the top-level Project's output, or because they are needed for ordering)
            expectedDataOutput = new ArrayList<>();
            for (Attribute a : physicalPlanOutput) {
                if (topLevelProject.outputSet().contains(a) || orderRefsSet.contains(a) || EsQueryExec.isDocAttribute(a)) {
                    expectedDataOutput.add(a);
                }
            }
            dataFragmentChild = withAddedDocToRelation;
        }
        var updatedFragment = new Project(Source.EMPTY, dataFragmentChild, expectedDataOutput);
        FragmentExec updatedFragmentExec = fragmentExec.withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);
        Source topNSource = topN.source();

        // Replace the TopN child with the data driver as the source.
        List<Attribute> reductionDeferredAttributes = deferredAttributes;
        PhysicalPlan reductionPlan = mappedReductionPlan.transformDown(TopNExec.class, t -> {
            PhysicalPlan exchangeExec = new ExchangeSourceExec(topNSource, expectedDataOutput, false /* isIntermediateAgg */);
            if (reductionDeferredAttributes.isEmpty() == false) {
                exchangeExec = materialize == null
                    ? MaterializeExec.local(topNSource, exchangeExec, doc, reductionDeferredAttributes, MaterializeTarget.CURRENT_FINAL)
                    : MaterializeExec.local(
                        topNSource,
                        exchangeExec,
                        doc,
                        materialize.carryAttributes(),
                        reductionDeferredAttributes,
                        MaterializeTarget.CURRENT_FINAL
                    );
            }
            // If the fragment is already sorted, tell the node-reduce TopN that its input will be sorted already
            boolean fragmentIsSorted = updatedFragment.child() instanceof TopN;
            return fragmentIsSorted ? t.replaceChild(exchangeExec).withSortedInput() : t.replaceChild(exchangeExec);
        });
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        ExchangeSinkExec reductionPlanWithSize = originalPlan.replaceChild(sizedReductionPlan);

        // The TopN reduction plan should not be further optimized locally on the node reduce driver, since we took great pains to
        // preplan in advance, including all the necessary field extractions!
        return Optional.of(new ReductionPlan(reductionPlanWithSize, updatedDataPlan, LocalPhysicalOptimization.DISABLED));
    }

    private static PhysicalPlan toPhysical(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        PhysicalPlan mapped = new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(plan));
        mapped = new LowerLocalMaterialize().apply(mapped, context);
        return new InsertFieldExtraction().apply(mapped, context);
    }

    static Optional<LogicalPlan> insertMaterializeBoundary(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        LogicalPlan fragment
    ) {
        return materializeBoundaryFragment(contextFactory, fragment, MaterializeTarget.CURRENT_FINAL).map(MaterializeBoundaryFragment::project);
    }

    static Optional<MaterializeBoundaryFragment> materializeBoundaryFragment(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        LogicalPlan fragment,
        MaterializeTarget target
    ) {
        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        List<Attribute> deferredAttributes = lateMaterializeAttributes(toPhysical(fragment, context));
        return materializeBoundaryFragment(fragment, deferredAttributes, target);
    }

    static Optional<MaterializeBoundaryFragment> materializeBoundaryFragment(
        LogicalPlan fragment,
        List<? extends Attribute> lateAttributes,
        MaterializeTarget target
    ) {
        return insertMaterializeBoundary(fragment, lateAttributes, target).flatMap(LateMaterializationPlanner::materializeBoundaryFragment);
    }

    static Optional<LogicalPlan> insertMaterializeBoundary(
        LogicalPlan fragment,
        List<? extends Attribute> lateAttributes,
        MaterializeTarget target
    ) {
        Project topLevelProject = fragment instanceof Project p ? p : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }
        TopN topN = topLevelProject.child() instanceof TopN tn ? tn : null;
        if (topN == null) {
            return Optional.empty();
        }
        if (topN.child() instanceof Materialize) {
            return Optional.of(fragment);
        }
        if (lateAttributes.isEmpty()) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(o -> o.references().stream()).toList());
        Attribute doc = new FieldAttribute(topN.source(), null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        LogicalPlan childWithDoc = topN.child().transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP || r.output().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                return r;
            }
            return r.withAttributes(CollectionUtils.prependToCopy(doc, r.output()));
        });
        TopN topNWithDoc = topN.replaceChild(childWithDoc);
        if (topNWithDoc.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        List<Attribute> outputAttributes = new ArrayList<>();
        for (Attribute attribute : topNWithDoc.output()) {
            if (EsQueryExec.isDocAttribute(attribute)
                || topLevelProject.outputSet().contains(attribute)
                || orderRefsSet.contains(attribute)) {
                outputAttributes.add(attribute);
            }
        }

        java.util.Set<String> deferredNames = lateAttributes.stream().map(Attribute::name).collect(Collectors.toSet());
        List<Attribute> materializeDeferredAttributes = outputAttributes.stream()
            .filter(attr -> EsQueryExec.isDocAttribute(attr) == false)
            .filter(attr -> deferredNames.contains(attr.name()))
            .toList();
        if (materializeDeferredAttributes.isEmpty()) {
            return Optional.empty();
        }

        AttributeSet deferredOutputSet = AttributeSet.of(materializeDeferredAttributes);
        List<Attribute> carryAttributes = outputAttributes.stream()
            .filter(attr -> EsQueryExec.isDocAttribute(attr) == false)
            .filter(attr -> deferredOutputSet.contains(attr) == false)
            .toList();
        Materialize materialize = new Materialize(
            topN.source(),
            topNWithDoc.child(),
            outputAttributes,
            doc,
            carryAttributes,
            materializeDeferredAttributes,
            target,
            MaterializeMode.LOCAL
        );
        return Optional.of(topLevelProject.replaceChild(topNWithDoc.replaceChild(materialize)));
    }

    private static Optional<MaterializeBoundaryFragment> materializeBoundaryFragment(LogicalPlan fragment) {
        if (fragment instanceof Project == false) {
            return Optional.empty();
        }
        Project project = (Project) fragment;
        if (project.child() instanceof TopN == false) {
            return Optional.empty();
        }
        TopN topN = (TopN) project.child();
        if (topN.child() instanceof Materialize == false) {
            return Optional.empty();
        }
        Materialize materialize = (Materialize) topN.child();
        return Optional.of(new MaterializeBoundaryFragment(project, topN, materialize));
    }

    record MaterializeBoundaryFragment(Project project, TopN topN, Materialize materialize) {
        List<Attribute> passthroughOutput() {
            return CollectionUtils.prependToCopy(materialize.rowIdentity(), materialize.carryAttributes());
        }

        TopN loweredTopN() {
            return topN.replaceChild(materialize.child());
        }

        List<NamedExpression> passthroughProjections(List<? extends NamedExpression> originalProjections) {
            java.util.Set<String> carryNames = materialize.carryAttributes().stream().map(Attribute::name).collect(Collectors.toSet());
            List<NamedExpression> passthroughProjections = originalProjections.stream()
                .filter(projection -> carryNames.contains(projection.name()))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toCollection(ArrayList::new));
            java.util.Set<String> passthroughProjectionNames = passthroughProjections.stream()
                .map(NamedExpression::name)
                .collect(Collectors.toSet());
            for (Attribute carryAttribute : materialize.carryAttributes()) {
                if (passthroughProjectionNames.add(carryAttribute.name())) {
                    passthroughProjections.add(carryAttribute);
                }
            }
            passthroughProjections.add(0, materialize.rowIdentity());
            return passthroughProjections;
        }
    }

    private static List<Attribute> lateMaterializeAttributes(PhysicalPlan plan) {
        return new ArrayList<>(
            plan.collect(FieldExtractExec.class::isInstance)
                .stream()
                .map(FieldExtractExec.class::cast)
                .filter(fieldExtract -> fieldExtract.child().anyMatch(TopNExec.class::isInstance))
                .flatMap(fieldExtract -> fieldExtract.attributesToExtract().stream())
                .collect(Collectors.toMap(Attribute::name, attribute -> attribute, (left, right) -> left, LinkedHashMap::new))
                .values()
        );
    }

    private LateMaterializationPlanner() { /* static class */ }

    // A hack to avoid the ReplaceFieldWithConstantOrNull optimization, since we don't have search stats during the reduce planning phase.
    // This sidesteps the issue by just assuming all fields exist and have no other meaningful stats. The local data optimizer will use the
    // real statistics.
    private static final SearchStats SEARCH_STATS_TOP_N_REPLACEMENT = new SearchStats.UnsupportedSearchStats() {
        @Override
        public boolean exists(FieldAttribute.FieldName field) {
            return true;
        }

        @Override
        public boolean isIndexed(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public Object min(FieldAttribute.FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldAttribute.FieldName field) {
            return null;
        }
    };
}

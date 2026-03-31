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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EmitRemoteFetchHandleExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Planner helpers for the distributed remote fetch prototype on the linear TopN family.
 */
final class RemoteFetchPlanner {
    static final String REMOTE_FETCH_HANDLE_NAME = "_remote_fetch_handle";

    static Optional<PhysicalPlan> planCoordinatorTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        PhysicalPlan coordinatorPlan,
        ExchangeSinkExec originalDataPlan
    ) {
        PlanningInfo info = analyze(contextFactory, originalDataPlan).orElse(null);
        if (info == null) {
            return Optional.empty();
        }
        return rewriteCoordinatorPlan(coordinatorPlan, info);
    }

    static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        PlanningInfo info = analyze(contextFactory, originalPlan).orElse(null);
        if (info == null) {
            return Optional.empty();
        }

        LogicalPlan withAddedDocToRelation = info.topN().transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return r;
            }
            List<Attribute> attributes = CollectionUtils.prependToCopy(info.docAttribute(), r.output());
            return r.withAttributes(attributes);
        });
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        Project updatedFragment = new Project(Source.EMPTY, withAddedDocToRelation, info.carryAttributes());
        FragmentExec updatedFragmentExec = info.fragmentExec().withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(updatedFragmentExec);

        PhysicalPlan reductionPlan = toPhysical(updatedFragment, info.context()).transformDown(
            TopNExec.class,
            topNExec -> topNExec.replaceChild(new ExchangeSourceExec(info.topN().source(), info.carryAttributes(), false)).withSortedInput()
        );
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan);
        PhysicalPlan encodedReductionPlan = new EmitRemoteFetchHandleExec(
            Source.EMPTY,
            sizedReductionPlan,
            info.docAttribute(),
            info.handleAttribute()
        );
        ExchangeSinkExec nodeReducePlan = new ExchangeSinkExec(
            originalPlan.source(),
            info.externalOutput(),
            originalPlan.isIntermediateAgg(),
            encodedReductionPlan
        );
        return Optional.of(new ReductionPlan(nodeReducePlan, updatedDataPlan, LocalPhysicalOptimization.DISABLED));
    }

    private static Optional<PlanningInfo> analyze(
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
        if (topN == null) {
            return Optional.empty();
        }

        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        List<Attribute> physicalPlanOutput = toPhysical(topN, context).output();
        Attribute doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(order -> order.references().stream()).toList());
        List<Attribute> carryAttributes = new ArrayList<>();
        for (Attribute attribute : physicalPlanOutput) {
            if (orderRefsSet.contains(attribute) || EsQueryExec.isDocAttribute(attribute)) {
                carryAttributes.add(attribute);
            }
        }
        if (carryAttributes.stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        ReferenceAttribute handle = new ReferenceAttribute(Source.EMPTY, null, REMOTE_FETCH_HANDLE_NAME, DataType.KEYWORD);
        return Optional.of(
            new PlanningInfo(fragmentExec, topN, context, doc, handle, carryAttributes, replaceDocAttribute(carryAttributes, doc, handle))
        );
    }

    private static Optional<PhysicalPlan> rewriteCoordinatorPlan(PhysicalPlan coordinatorPlan, PlanningInfo info) {
        if (coordinatorPlan instanceof OutputExec outputExec) {
            return rewriteCoordinatorPlan(outputExec.child(), info).map(outputExec::replaceChild);
        }
        if (coordinatorPlan instanceof ProjectExec == false) {
            return Optional.empty();
        }
        ProjectExec projectExec = (ProjectExec) coordinatorPlan;

        List<ExchangeSourceExec> sources = projectExec.child().collect(ExchangeSourceExec.class);
        if (sources.size() != 1) {
            return Optional.empty();
        }
        PhysicalPlan updatedChild = projectExec.child()
            .transformDown(
                ExchangeSourceExec.class,
                exchangeSource -> new ExchangeSourceExec(exchangeSource.source(), info.externalOutput(), exchangeSource.isIntermediateAgg())
            );
        List<Attribute> missingAttributes = projectExec.references()
            .stream()
            .filter(attribute -> updatedChild.outputSet().contains(attribute) == false)
            .toList();
        if (missingAttributes.isEmpty()) {
            return Optional.empty();
        }
        if (missingAttributes.stream().allMatch(RemoteFetchPlanner::isRemoteFetchableAttribute) == false) {
            return Optional.empty();
        }
        return Optional.of(
            projectExec.replaceChild(new RemoteFetchExec(projectExec.source(), updatedChild, info.handleAttribute(), missingAttributes))
        );
    }

    private static boolean isRemoteFetchableAttribute(Attribute attribute) {
        if (attribute instanceof FieldAttribute) {
            return true;
        }
        return attribute instanceof MetadataAttribute
            && MetadataAttribute.SCORE.equals(attribute.name()) == false
            && EsQueryExec.isDocAttribute(attribute) == false;
    }

    private static List<Attribute> replaceDocAttribute(List<Attribute> attributes, Attribute docAttribute, Attribute handleAttribute) {
        List<Attribute> replaced = new ArrayList<>(attributes.size());
        for (Attribute attribute : attributes) {
            replaced.add(attribute.id().equals(docAttribute.id()) ? handleAttribute : attribute);
        }
        return replaced;
    }

    private static PhysicalPlan toPhysical(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(plan)), context);
    }

    private record PlanningInfo(
        FragmentExec fragmentExec,
        TopN topN,
        LocalPhysicalOptimizerContext context,
        Attribute docAttribute,
        ReferenceAttribute handleAttribute,
        List<Attribute> carryAttributes,
        List<Attribute> externalOutput
    ) {}

    private RemoteFetchPlanner() {}

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.FieldExtractionSpec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.FetchHandle;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

/**
 * Places an explicit fetch boundary around an eligible distributed TopN so fields can be fetched after final reduction.
 */
public final class PlanDeferredFetch extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (context.configuration().pragmas().fetchTopN() == false
            || hasOnlyLocalConcreteIndices(plan) == false
            || plan.collect(TopNExec.class).size() != 1
            || context.minimumVersion().supports(FetchBoundaryExec.ESQL_FETCH_BOUNDARY) == false) {
            return plan;
        }

        // This context is used only to derive the reduction schema. The invoked rules consult the request configuration and
        // synthetic search statistics, but not cluster-level planner settings or flags.
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory = stats -> new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(false),
            context.configuration(),
            context.configuration().newFoldContext(),
            stats
        );
        return rewriteTopN(contextFactory, plan, context.minimumVersion());
    }

    private static boolean hasOnlyLocalConcreteIndices(PhysicalPlan plan) {
        Set<String> clusters = new HashSet<>();
        var hasConcreteIndices = new Holder<Boolean>(false);
        PlannerUtils.forEachRelation(plan, relation -> {
            clusters.addAll(relation.concreteIndices().keySet());
            if (relation.concreteIndices().values().stream().anyMatch(indices -> indices.isEmpty() == false)) {
                hasConcreteIndices.set(true);
            }
        });
        return Boolean.TRUE.equals(hasConcreteIndices.get()) && clusters.size() == 1 && clusters.contains(LOCAL_CLUSTER_GROUP_KEY);
    }

    private static PhysicalPlan rewriteTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        PhysicalPlan plan,
        TransportVersion minimumTransportVersion
    ) {
        List<Attribute> originalOutput = plan.output();
        var replacedTopN = new Holder<Boolean>(false);
        PhysicalPlan rewrittenPlan = plan.transformDown(TopNExec.class, topNExec -> {
            if (replacedTopN.get() || (topNExec.child() instanceof ExchangeExec) == false) {
                return topNExec;
            }
            ExchangeExec exchange = (ExchangeExec) topNExec.child();
            FetchTopNContext planningContext = analyzeTopN(contextFactory, exchange.child()).orElse(null);
            if (planningContext == null) {
                return topNExec;
            }
            FragmentExec fragmentExec = planningContext.fragmentExec();
            Project topLevelProject = planningContext.topLevelProject();
            List<Attribute> expectedDataOutput = planningContext.expectedDataOutput();

            List<Attribute> exchangeOutput = new ArrayList<>();
            Attribute handle = fetchHandleAttribute(topNExec.source());
            exchangeOutput.add(handle);
            for (Attribute attr : expectedDataOutput) {
                if (EsQueryExec.isDocAttribute(attr) == false) {
                    exchangeOutput.add(attr);
                }
            }

            AttributeSet exchangeOutputSet = AttributeSet.of(exchangeOutput);
            List<Attribute> attributesToFetch = new ArrayList<>();
            List<FieldExtractionSpec> extractionSpecs = new ArrayList<>();
            for (Attribute attr : topLevelProject.output()) {
                if (exchangeOutputSet.contains(attr) == false) {
                    FieldExtractionSpec extractionSpec = FieldExtractionSpec.plan(
                        attr,
                        planningContext.optimizerContext().configuration().pragmas().fieldExtractPreference()
                    ).orElse(null);
                    if (extractionSpec == null || extractionSpec.supports(minimumTransportVersion) == false) {
                        return topNExec;
                    }
                    attributesToFetch.add(attr);
                    extractionSpecs.add(extractionSpec);
                }
            }
            if (attributesToFetch.isEmpty()) {
                return topNExec;
            }

            FragmentExec updatedFragmentExec = fragmentExec.withFragment(
                new Project(Source.EMPTY, planningContext.withAddedDocToRelation(), expectedDataOutput)
            );
            FetchBoundaryExec fetchBoundary = new FetchBoundaryExec(exchange.source(), updatedFragmentExec, handle, exchangeOutput);
            ExchangeExec updatedExchange = new ExchangeExec(exchange.source(), exchangeOutput, exchange.inBetweenAggs(), fetchBoundary);
            FragmentExec fetchPlan = new FragmentExec(new FetchSource(Source.EMPTY, attributesToFetch));
            replacedTopN.set(true);
            TopNExec updatedTopN = topNExec.replaceChild(updatedExchange);
            return new FetchExec(topNExec.source(), updatedTopN, handle, attributesToFetch, extractionSpecs, attributesToFetch, fetchPlan);
        });
        if (replacedTopN.get() && rewrittenPlan.output().equals(originalOutput) == false) {
            return new ProjectExec(plan.source(), rewrittenPlan, originalOutput);
        }
        return rewrittenPlan;
    }

    private record FetchTopNContext(
        FragmentExec fragmentExec,
        Project topLevelProject,
        LogicalPlan withAddedDocToRelation,
        List<Attribute> expectedDataOutput,
        LocalPhysicalOptimizerContext optimizerContext
    ) {
        private FetchTopNContext {
            expectedDataOutput = List.copyOf(expectedDataOutput);
        }
    }

    private static Optional<FetchTopNContext> analyzeTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        PhysicalPlan exchangeChild
    ) {
        FragmentExec fragmentExec = exchangeChild instanceof FragmentExec fe ? fe : null;
        if (fragmentExec == null) {
            return Optional.empty();
        }
        Project topLevelProject = fragmentExec.fragment() instanceof Project p ? p : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }
        TopN topN = topLevelProject.child() instanceof TopN tn ? tn : null;
        if (topN == null || topN.child().anyMatch(PipelineBreaker.class::isInstance)) {
            return Optional.empty();
        }

        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        List<Attribute> physicalPlanOutput = toPhysicalPlanForFetchSchema(topN, context).output();
        Attribute doc = physicalPlanOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return Optional.empty();
        }

        LogicalPlan withAddedDocToRelation = topN.transformUp(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP || relation.outputSet().contains(doc)) {
                return relation;
            }
            return relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output()));
        });
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            return Optional.empty();
        }

        AttributeSet orderRefsSet = AttributeSet.of(topN.order().stream().flatMap(order -> order.references().stream()).toList());
        List<Attribute> expectedDataOutput = new ArrayList<>();
        for (Attribute attr : physicalPlanOutput) {
            if (topLevelProject.outputSet().contains(attr) || orderRefsSet.contains(attr) || EsQueryExec.isDocAttribute(attr)) {
                expectedDataOutput.add(attr);
            }
        }
        return Optional.of(new FetchTopNContext(fragmentExec, topLevelProject, withAddedDocToRelation, expectedDataOutput, context));
    }

    private static PhysicalPlan toPhysicalPlanForFetchSchema(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        var logicalContext = new LocalLogicalOptimizerContext(context.configuration(), context.foldCtx(), context.searchStats());
        LogicalPlan optimized = new ReplaceFieldWithConstantOrNull().apply(plan, logicalContext);
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(LocalMapper.INSTANCE.map(optimized)), context);
    }

    private static Attribute fetchHandleAttribute(Source source) {
        return new ReferenceAttribute(source, null, FetchHandle.ATTRIBUTE_NAME, DataType.KEYWORD, Nullability.FALSE, null, true);
    }

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

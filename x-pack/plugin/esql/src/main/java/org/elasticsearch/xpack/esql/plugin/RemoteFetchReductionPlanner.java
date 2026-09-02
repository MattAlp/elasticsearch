/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.RemoteFetchHandleFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Builds the existing node-reduce implementation from the authoritative remote-fetch boundary planned by the coordinator.
 * Task 2 replaces this compatibility planner with a local optimizer rule.
 */
final class RemoteFetchReductionPlanner {

    /**
     * Request-specific identity embedded in handles emitted by this data node.
     */
    record RemoteFetchContext(String localNodeId, String retainedSessionId) {
        RemoteFetchContext {
            Objects.requireNonNull(localNodeId, "localNodeId");
            Objects.requireNonNull(retainedSessionId, "retainedSessionId");
        }
    }

    static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan,
        RemoteFetchContext remoteFetchContext
    ) {
        if (originalPlan.child() instanceof RemoteFetchBoundaryExec == false) {
            return Optional.empty();
        }
        RemoteFetchBoundaryExec boundary = (RemoteFetchBoundaryExec) originalPlan.child();
        if (boundary.child() instanceof FragmentExec == false) {
            return Optional.empty();
        }
        FragmentExec fragmentExec = (FragmentExec) boundary.child();
        if (fragmentExec.fragment() instanceof Project == false) {
            return Optional.empty();
        }
        Project project = (Project) fragmentExec.fragment();
        if (project.child() instanceof TopN == false) {
            return Optional.empty();
        }
        TopN topN = (TopN) project.child();
        if (originalPlan.output().equals(boundary.handoffOutput()) == false) {
            throw new IllegalStateException(
                "remote-fetch boundary handoff output "
                    + boundary.handoffOutput()
                    + " does not match exchange output "
                    + originalPlan.output()
            );
        }

        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChildAndUpdateOutput(fragmentExec);
        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        PhysicalPlan reductionPlan = PlannerUtils.toPhysicalPlanForReductionSchema(fragmentExec.fragment(), context)
            .transformDown(
                TopNExec.class,
                exec -> exec.replaceChild(new ExchangeSourceExec(topN.source(), boundary.dataOutput(), false)).withSortedInput()
            );
        Alias handleAlias = new Alias(
            Source.EMPTY,
            boundary.handleAttribute().name(),
            new RemoteFetchHandleFunction(
                Source.EMPTY,
                boundary.documentAttribute(),
                remoteFetchContext.localNodeId(),
                remoteFetchContext.retainedSessionId()
            ),
            boundary.handleAttribute().id(),
            true
        );
        PhysicalPlan withHandle = new EvalExec(Source.EMPTY, reductionPlan, List.of(handleAlias));
        PhysicalPlan projected = new ProjectExec(Source.EMPTY, withHandle, boundary.handoffOutput());
        PhysicalPlan sizedReductionPlan = EstimatesRowSize.estimateRowSize(fragmentExec.estimatedRowSize(), projected);
        return Optional.of(new ReductionPlan(originalPlan.replaceChild(sizedReductionPlan), updatedDataPlan));
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

    private RemoteFetchReductionPlanner() {}
}

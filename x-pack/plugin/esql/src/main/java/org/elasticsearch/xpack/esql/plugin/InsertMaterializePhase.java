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
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Materialize;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeMode;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeTarget;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final class InsertMaterializePhase extends ParameterizedRuleExecutor<LogicalPlan, InsertMaterializePhase.Context> {
    private static final List<Batch<LogicalPlan>> RULES = List.of(new Batch<>("Insert Materialize", Limiter.ONCE, new InsertMaterialize()));

    private InsertMaterializePhase(Context optimizerContext) {
        super(optimizerContext);
    }

    static Optional<LogicalPlan> apply(LocalPhysicalOptimizerContext context, LogicalPlan fragment, MaterializeTarget target) {
        return apply(fragment, Context.fromPhysicalContext(context, target));
    }

    static Optional<LogicalPlan> apply(LogicalPlan fragment, List<? extends Attribute> lateAttributes, MaterializeTarget target) {
        return apply(fragment, Context.fromDeferredAttributes(lateAttributes, target));
    }

    static Optional<LateMaterializationPlanner.MaterializeBoundaryFragment> materializeBoundaryFragment(
        LocalPhysicalOptimizerContext context,
        LogicalPlan fragment,
        MaterializeTarget target
    ) {
        return apply(context, fragment, target).flatMap(InsertMaterializePhase::materializeBoundaryFragment);
    }

    static Optional<LateMaterializationPlanner.MaterializeBoundaryFragment> materializeBoundaryFragment(
        LogicalPlan fragment,
        List<? extends Attribute> lateAttributes,
        MaterializeTarget target
    ) {
        return apply(fragment, lateAttributes, target).flatMap(InsertMaterializePhase::materializeBoundaryFragment);
    }

    static Optional<LateMaterializationPlanner.MaterializeBoundaryFragment> materializeBoundaryFragment(LogicalPlan fragment) {
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
        return Optional.of(new LateMaterializationPlanner.MaterializeBoundaryFragment(project, topN, materialize));
    }

    private static Optional<LogicalPlan> apply(LogicalPlan fragment, Context context) {
        LogicalPlan updatedFragment = new InsertMaterializePhase(context).execute(fragment);
        return materializeBoundaryFragment(updatedFragment).map(LateMaterializationPlanner.MaterializeBoundaryFragment::project);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    private static Optional<LogicalPlan> insertMaterializeBoundary(
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
        if (topN.child() instanceof Materialize materialize) {
            if (materialize.target() == target) {
                return Optional.of(fragment);
            }
            topN = topN.replaceChild(materialize.child());
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

    record Context(LocalPhysicalOptimizerContext physicalContext, List<Attribute> lateAttributes, MaterializeTarget target) {
        Context {
            if (physicalContext == null && lateAttributes == null) {
                throw new IllegalArgumentException("expected physical context or deferred attributes");
            }
            lateAttributes = lateAttributes == null ? null : List.copyOf(lateAttributes);
        }

        private static Context fromPhysicalContext(LocalPhysicalOptimizerContext physicalContext, MaterializeTarget target) {
            return new Context(physicalContext, null, target);
        }

        private static Context fromDeferredAttributes(List<? extends Attribute> lateAttributes, MaterializeTarget target) {
            return new Context(null, lateAttributes.stream().map(Attribute.class::cast).toList(), target);
        }

        private List<Attribute> lateAttributes(LogicalPlan fragment) {
            if (lateAttributes != null) {
                return lateAttributes;
            }
            return LateMaterializationPlanner.lateMaterializeAttributes(LateMaterializationPlanner.toPhysical(fragment, physicalContext));
        }
    }

    private static class InsertMaterialize extends ParameterizedRule<LogicalPlan, LogicalPlan, Context> {
        @Override
        public LogicalPlan apply(LogicalPlan fragment, Context context) {
            return insertMaterializeBoundary(fragment, context.lateAttributes(fragment), context.target).orElse(fragment);
        }
    }
}

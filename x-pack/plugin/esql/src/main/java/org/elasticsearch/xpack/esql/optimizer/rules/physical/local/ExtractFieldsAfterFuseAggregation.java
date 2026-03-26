/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Replaces field-carrying {@code VALUES(field)} aggregations in FUSE's coordinator-side aggregation with
 * {@code FIRST_DOC_ID(_doc)} so later operators can materialize those fields after post-FUSE narrowing operators.
 */
public final class ExtractFieldsAfterFuseAggregation extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if (plan instanceof AggregateExec oldAgg
            && oldAgg.getMode() == AggregatorMode.SINGLE
            && oldAgg.child() instanceof FuseScoreEvalExec) {
            return rule(oldAgg);
        }
        return plan;
    }

    private PhysicalPlan rule(AggregateExec oldAgg) {
        Attribute sourceAttr = oldAgg.child().output().stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (sourceAttr == null) {
            return oldAgg;
        }

        Set<String> groupingNames = oldAgg.groupings()
            .stream()
            .filter(Attribute.class::isInstance)
            .map(Attribute.class::cast)
            .map(Attribute::name)
            .collect(Collectors.toSet());
        List<NamedExpression> newAggregates = new ArrayList<>(oldAgg.aggregates().size() + 1);
        boolean removedAnyFieldValues = false;
        boolean keepsDoc = false;

        for (NamedExpression aggregate : oldAgg.aggregates()) {
            if (aggregate instanceof Alias alias) {
                if (alias.child() instanceof Values values
                    && groupingNames.contains(alias.name()) == false
                    && values.field() instanceof Attribute attribute
                    && attribute instanceof MetadataAttribute == false
                    && EsQueryExec.isDocAttribute(attribute) == false) {
                    removedAnyFieldValues = true;
                    continue;
                }
                if (EsQueryExec.isDocAttribute(alias.toAttribute())) {
                    keepsDoc = true;
                }
            }
            newAggregates.add(aggregate);
        }

        if (removedAnyFieldValues == false || keepsDoc) {
            return oldAgg;
        }

        newAggregates.add(new Alias(oldAgg.source(), sourceAttr.name(), new FirstDocId(oldAgg.source(), sourceAttr), sourceAttr.id()));
        return oldAgg.withAggregates(newAggregates);
    }
}

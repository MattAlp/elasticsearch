/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MaterializeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rewrites a local {@link MaterializeExec} boundary so its child carries only
 * row identity plus pass-through columns. Deferred fields remain referenced
 * above the boundary and are loaded later by {@link InsertFieldExtraction}.
 */
public final class LowerLocalMaterialize extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        return plan.transformDown(MaterializeExec.class, LowerLocalMaterialize::lower);
    }

    private static PhysicalPlan lower(MaterializeExec materialize) {
        List<Attribute> passthroughOutput = passthroughOutput(materialize);
        if (materialize.child().output().equals(passthroughOutput)) {
            return materialize;
        }

        PhysicalPlan loweredChild = materialize.child() instanceof ExchangeSourceExec exchangeSource
            ? new ExchangeSourceExec(exchangeSource.source(), passthroughOutput, exchangeSource.isIntermediateAgg())
            : new ProjectExec(materialize.source(), materialize.child(), passthroughOutput);
        return materialize.replaceChild(loweredChild);
    }

    private static List<Attribute> passthroughOutput(MaterializeExec materialize) {
        Set<Attribute> carrySet = new HashSet<>(materialize.carryAttributes());
        List<Attribute> passthroughOutput = new ArrayList<>(materialize.carryAttributes().size() + 1);
        for (Attribute attribute : materialize.child().output()) {
            if (attribute.equals(materialize.rowIdentity())) {
                passthroughOutput.add(attribute);
            } else if (carrySet.contains(attribute)) {
                passthroughOutput.add(attribute);
            }
        }
        return passthroughOutput;
    }
}

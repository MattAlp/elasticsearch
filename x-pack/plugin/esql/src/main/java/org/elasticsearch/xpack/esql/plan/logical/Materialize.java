/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeMode;
import org.elasticsearch.xpack.esql.plan.materialize.MaterializeTarget;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Planner marker for a late-materialization boundary.
 * <p>
 * Logically, {@link #output()} preserves the columns expected by upstream
 * operators, including deferred attributes. Physically, the corresponding
 * {@code MaterializeExec} can lower to a child that only carries row identity
 * plus pass-through attributes, allowing later local planning to insert field
 * extraction exactly where it is first needed.
 */
public class Materialize extends UnaryPlan implements ExecutesOn.Coordinator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Materialize",
        Materialize::new
    );

    private final List<Attribute> outputAttributes;
    private final Attribute rowIdentity;
    private final List<Attribute> carryAttributes;
    private final List<Attribute> deferredAttributes;
    private final MaterializeTarget target;
    private final MaterializeMode mode;

    public Materialize(
        Source source,
        LogicalPlan child,
        List<Attribute> outputAttributes,
        Attribute rowIdentity,
        List<Attribute> carryAttributes,
        List<Attribute> deferredAttributes,
        MaterializeTarget target,
        MaterializeMode mode
    ) {
        super(source, child);
        this.outputAttributes = List.copyOf(outputAttributes);
        this.rowIdentity = rowIdentity;
        this.carryAttributes = List.copyOf(carryAttributes);
        this.deferredAttributes = List.copyOf(deferredAttributes);
        this.target = target;
        this.mode = mode;
    }

    private Materialize(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readEnum(MaterializeTarget.class),
            in.readEnum(MaterializeMode.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(outputAttributes);
        out.writeNamedWriteable(rowIdentity);
        out.writeNamedWriteableCollection(carryAttributes);
        out.writeNamedWriteableCollection(deferredAttributes);
        out.writeEnum(target);
        out.writeEnum(mode);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Materialize> info() {
        return NodeInfo.create(
            this,
            Materialize::new,
            child(),
            outputAttributes,
            rowIdentity,
            carryAttributes,
            deferredAttributes,
            target,
            mode
        );
    }

    @Override
    public Materialize replaceChild(LogicalPlan newChild) {
        return new Materialize(source(), newChild, outputAttributes, rowIdentity, carryAttributes, deferredAttributes, target, mode);
    }

    @Override
    public boolean expressionsResolved() {
        return rowIdentity.resolved()
            && Resolvables.resolved(outputAttributes)
            && Resolvables.resolved(carryAttributes)
            && Resolvables.resolved(deferredAttributes);
    }

    @Override
    public List<Attribute> output() {
        return outputAttributes;
    }

    @Override
    protected AttributeSet computeReferences() {
        List<Attribute> references = new ArrayList<>(carryAttributes.size() + 1);
        references.add(rowIdentity);
        references.addAll(carryAttributes);
        return AttributeSet.of(references);
    }

    public Attribute rowIdentity() {
        return rowIdentity;
    }

    public List<Attribute> carryAttributes() {
        return carryAttributes;
    }

    public List<Attribute> deferredAttributes() {
        return deferredAttributes;
    }

    public MaterializeTarget target() {
        return target;
    }

    public MaterializeMode mode() {
        return mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputAttributes, rowIdentity, carryAttributes, deferredAttributes, target, mode, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Materialize other = (Materialize) obj;
        return Objects.equals(outputAttributes, other.outputAttributes)
            && Objects.equals(rowIdentity, other.rowIdentity)
            && Objects.equals(carryAttributes, other.carryAttributes)
            && Objects.equals(deferredAttributes, other.deferredAttributes)
            && target == other.target
            && mode == other.mode
            && Objects.equals(child(), other.child());
    }
}

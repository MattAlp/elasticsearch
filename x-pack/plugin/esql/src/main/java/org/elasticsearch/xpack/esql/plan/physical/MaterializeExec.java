/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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
 * Physical marker for a late-materialization boundary. This node is a planning
 * artifact only and does not correspond to a dedicated runtime operator yet.
 */
public class MaterializeExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "MaterializeExec",
        MaterializeExec::new
    );

    private final Attribute rowIdentity;
    private final List<Attribute> carryAttributes;
    private final List<Attribute> deferredAttributes;
    private final MaterializeTarget target;
    private final MaterializeMode mode;

    public MaterializeExec(
        Source source,
        PhysicalPlan child,
        Attribute rowIdentity,
        List<Attribute> carryAttributes,
        List<Attribute> deferredAttributes,
        MaterializeTarget target,
        MaterializeMode mode
    ) {
        super(source, child);
        this.rowIdentity = rowIdentity;
        this.carryAttributes = List.copyOf(carryAttributes);
        this.deferredAttributes = List.copyOf(deferredAttributes);
        this.target = target;
        this.mode = mode;
    }

    public static MaterializeExec local(
        Source source,
        PhysicalPlan child,
        Attribute rowIdentity,
        List<? extends Attribute> deferredAttributes,
        MaterializeTarget target
    ) {
        List<Attribute> carryAttributes = child.output().stream().filter(attr -> EsQueryExec.isDocAttribute(attr) == false).toList();
        return new MaterializeExec(
            source,
            child,
            rowIdentity,
            carryAttributes,
            List.copyOf(deferredAttributes),
            target,
            MaterializeMode.LOCAL
        );
    }

    private MaterializeExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
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
    protected AttributeSet computeReferences() {
        List<Attribute> references = new ArrayList<>(carryAttributes.size() + 1);
        references.add(rowIdentity);
        references.addAll(carryAttributes);
        return AttributeSet.of(references);
    }

    @Override
    protected NodeInfo<MaterializeExec> info() {
        return NodeInfo.create(this, MaterializeExec::new, child(), rowIdentity, carryAttributes, deferredAttributes, target, mode);
    }

    @Override
    public MaterializeExec replaceChild(PhysicalPlan newChild) {
        return new MaterializeExec(source(), newChild, rowIdentity, carryAttributes, deferredAttributes, target, mode);
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
    public String nodeString(NodeStringFormat format) {
        return Strings.format(
            "%s[row=%s, carry=%s, deferred=%s, target=%s, mode=%s]",
            nodeName(),
            rowIdentity.name(),
            Expressions.names(carryAttributes),
            Expressions.names(deferredAttributes),
            target,
            mode
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowIdentity, carryAttributes, deferredAttributes, target, mode, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MaterializeExec other = (MaterializeExec) obj;
        return Objects.equals(rowIdentity, other.rowIdentity)
            && Objects.equals(carryAttributes, other.carryAttributes)
            && Objects.equals(deferredAttributes, other.deferredAttributes)
            && target == other.target
            && mode == other.mode
            && Objects.equals(child(), other.child());
    }
}

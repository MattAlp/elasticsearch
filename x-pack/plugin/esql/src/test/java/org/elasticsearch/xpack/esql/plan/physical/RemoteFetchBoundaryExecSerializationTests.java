/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchBoundaryExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RemoteFetchBoundaryExec> {

    @Override
    protected RemoteFetchBoundaryExec createTestInstance() {
        Attribute doc = doc();
        Attribute handle = handle();
        List<Attribute> eager = randomFieldAttributes(1, 4, false);
        PhysicalPlan child = new ExchangeSourceExec(randomSource(), dataOutput(doc, eager), false);
        return new RemoteFetchBoundaryExec(randomSource(), child, doc, handle, eager);
    }

    public void testDefinesSeparateDataAndHandoffContracts() {
        RemoteFetchBoundaryExec boundary = createTestInstance();

        assertThat(boundary.dataOutput(), equalTo(boundary.child().output()));
        assertThat(boundary.dataOutput(), equalTo(dataOutput(boundary.documentAttribute(), boundary.eagerAttributes())));
        assertThat(boundary.handoffOutput(), equalTo(handoffOutput(boundary.handleAttribute(), boundary.eagerAttributes())));
        assertThat(boundary.output(), equalTo(boundary.handoffOutput()));
        assertTrue(RemoteFetchHandle.isRemoteFetchHandleCarrier(boundary.handleAttribute()));
        assertTrue(boundary.requiresRetainedSearchContexts());
        assertThat(boundary.minimumTransportVersion(), equalTo(RemoteFetchBoundaryExec.ESQL_REMOTE_FETCH_TOPN_REDUCTION));
    }

    public void testRejectsChildThatDoesNotProvideDataContract() {
        Attribute doc = doc();
        Attribute handle = handle();
        List<Attribute> eager = randomFieldAttributes(1, 4, false);
        PhysicalPlan child = new ExchangeSourceExec(randomSource(), eager, false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchBoundaryExec(randomSource(), child, doc, handle, eager)
        );
        assertThat(e.getMessage(), containsString("child output must match remote-fetch data output"));
    }

    public void testRejectsInvalidDocumentAttribute() {
        Attribute notDoc = randomFieldAttributes(1, 1, false).getFirst();
        Attribute handle = handle();
        List<Attribute> eager = randomFieldAttributes(1, 4, false);
        PhysicalPlan child = new ExchangeSourceExec(randomSource(), dataOutput(notDoc, eager), false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchBoundaryExec(randomSource(), child, notDoc, handle, eager)
        );
        assertThat(e.getMessage(), containsString("document attribute must be _doc"));
    }

    public void testRejectsNonSyntheticHandleAttribute() {
        Attribute doc = doc();
        Attribute invalidHandle = new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            false
        );
        List<Attribute> eager = randomFieldAttributes(1, 4, false);
        PhysicalPlan child = new ExchangeSourceExec(randomSource(), dataOutput(doc, eager), false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchBoundaryExec(randomSource(), child, doc, invalidHandle, eager)
        );
        assertThat(e.getMessage(), containsString("invalid remote-fetch handle attribute"));
    }

    @Override
    protected RemoteFetchBoundaryExec mutateInstance(RemoteFetchBoundaryExec instance) throws IOException {
        Attribute doc = instance.documentAttribute();
        Attribute handle = instance.handleAttribute();
        List<Attribute> eager = instance.eagerAttributes();
        switch (between(0, 2)) {
            case 0 -> doc = new MetadataAttribute(randomSource(), MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, randomBoolean());
            case 1 -> handle = handle();
            case 2 -> eager = randomValueOtherThan(eager, () -> randomFieldAttributes(1, 4, false));
            default -> throw new AssertionError("unexpected mutation branch");
        }
        PhysicalPlan child = new ExchangeSourceExec(randomSource(), dataOutput(doc, eager), false);
        return new RemoteFetchBoundaryExec(instance.source(), child, doc, handle, eager);
    }

    private static Attribute doc() {
        return new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
    }

    private static Attribute handle() {
        return new ReferenceAttribute(
            Source.EMPTY,
            null,
            RemoteFetchHandle.ATTRIBUTE_NAME,
            DataType.KEYWORD,
            Nullability.FALSE,
            null,
            true
        );
    }

    private static List<Attribute> dataOutput(Attribute doc, List<Attribute> eager) {
        List<Attribute> output = new ArrayList<>(eager.size() + 1);
        output.add(doc);
        output.addAll(eager);
        return output;
    }

    private static List<Attribute> handoffOutput(Attribute handle, List<Attribute> eager) {
        List<Attribute> output = new ArrayList<>(eager.size() + 1);
        output.add(handle);
        output.addAll(eager);
        return output;
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}

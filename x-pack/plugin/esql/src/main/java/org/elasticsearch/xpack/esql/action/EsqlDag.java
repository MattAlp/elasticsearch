/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record EsqlDag(List<EsqlDagEdge> edges) implements Writeable, ToXContentObject {

    public static final EsqlDag EMPTY = new EsqlDag(List.of());

    public static EsqlDag from(StreamInput in) throws IOException {
        return new EsqlDag(in.readCollectionAsList(EsqlDagEdge::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(edges);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("edges", edges);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlDag esqlDag = (EsqlDag) o;
        return Objects.equals(edges, esqlDag.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges);
    }
}
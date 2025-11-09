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
import java.util.Objects;

public record EsqlDagEdge(String sourceSessionId, String sourceNodeId, String sinkSessionId, String sinkNodeId)
    implements
        Writeable,
        ToXContentObject {

    public static EsqlDagEdge readFrom(StreamInput in) throws IOException {
        return new EsqlDagEdge(in.readString(), in.readString(), in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceSessionId);
        out.writeString(sourceNodeId);
        out.writeString(sinkSessionId);
        out.writeString(sinkNodeId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("source_session_id", sourceSessionId);
        builder.field("source_node_id", sourceNodeId);
        builder.field("sink_session_id", sinkSessionId);
        builder.field("sink_node_id", sinkNodeId);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlDagEdge that = (EsqlDagEdge) o;
        return Objects.equals(sourceSessionId, that.sourceSessionId)
            && Objects.equals(sourceNodeId, that.sourceNodeId)
            && Objects.equals(sinkSessionId, that.sinkSessionId)
            && Objects.equals(sinkNodeId, that.sinkNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceSessionId, sourceNodeId, sinkSessionId, sinkNodeId);
    }
}
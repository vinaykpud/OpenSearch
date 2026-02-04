/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.exec;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Aggregate node - groups and aggregates rows.
 */
public class ExecAggregate implements ExecNode {

    private final ExecNode input;
    private final List<String> groupBy;
    private final List<String> aggregates;  // e.g., "SUM(amount)", "COUNT(*)"
    private final AggMode mode;

    public enum AggMode {
        PARTIAL,  // Shard-level partial aggregation
        FINAL,    // Coordinator-level final aggregation
        FULL      // Single-node full aggregation
    }

    public ExecAggregate(ExecNode input, List<String> groupBy, List<String> aggregates, AggMode mode) {
        this.input = input;
        this.groupBy = groupBy;
        this.aggregates = aggregates;
        this.mode = mode;
    }

    public ExecAggregate(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.groupBy = in.readStringList();
        this.aggregates = in.readStringList();
        this.mode = AggMode.values()[in.readVInt()];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeStringCollection(groupBy);
        out.writeStringCollection(aggregates);
        out.writeVInt(mode.ordinal());
    }

    @Override
    public NodeType getType() {
        return NodeType.AGGREGATE;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public List<String> getAggregates() {
        return aggregates;
    }

    public AggMode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "ExecAggregate[groupBy=" + groupBy + ", aggs=" + aggregates + ", mode=" + mode + "]";
    }
}

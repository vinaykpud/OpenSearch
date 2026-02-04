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
 * Limit node - limits number of rows.
 */
public class ExecLimit implements ExecNode {

    private final ExecNode input;
    private final int limit;
    private final int offset;

    public ExecLimit(ExecNode input, int limit) {
        this(input, limit, 0);
    }

    public ExecLimit(ExecNode input, int limit, int offset) {
        this.input = input;
        this.limit = limit;
        this.offset = offset;
    }

    public ExecLimit(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.limit = in.readVInt();
        this.offset = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeVInt(limit);
        out.writeVInt(offset);
    }

    @Override
    public NodeType getType() {
        return NodeType.LIMIT;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "ExecLimit[" + limit + (offset > 0 ? ", offset=" + offset : "") + "]";
    }
}

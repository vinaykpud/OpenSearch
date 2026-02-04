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
 * Filter node - filters rows based on a condition.
 */
public class ExecFilter implements ExecNode {

    private final ExecNode input;
    private final String condition;  // Simple string condition for now

    public ExecFilter(ExecNode input, String condition) {
        this.input = input;
        this.condition = condition;
    }

    public ExecFilter(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.condition = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeString(condition);
    }

    @Override
    public NodeType getType() {
        return NodeType.FILTER;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public String getCondition() {
        return condition;
    }

    @Override
    public String toString() {
        return "ExecFilter[" + condition + "]";
    }
}

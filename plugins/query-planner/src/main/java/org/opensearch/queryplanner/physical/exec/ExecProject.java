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
 * Project node - selects columns.
 */
public class ExecProject implements ExecNode {

    private final ExecNode input;
    private final List<String> columns;

    public ExecProject(ExecNode input, List<String> columns) {
        this.input = input;
        this.columns = columns;
    }

    public ExecProject(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.columns = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeStringCollection(columns);
    }

    @Override
    public NodeType getType() {
        return NodeType.PROJECT;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "ExecProject" + columns;
    }
}

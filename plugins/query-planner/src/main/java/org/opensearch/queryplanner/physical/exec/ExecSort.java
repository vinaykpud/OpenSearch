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
 * Sort node - orders rows by specified columns.
 *
 * <p>Supports:
 * <ul>
 *   <li>Multiple sort columns</li>
 *   <li>ASC/DESC per column</li>
 *   <li>Optional LIMIT (top-N optimization)</li>
 *   <li>Optional OFFSET</li>
 * </ul>
 */
public class ExecSort implements ExecNode {

    private final ExecNode input;
    private final List<String> sortColumns;
    private final List<Boolean> ascending;  // true = ASC, false = DESC
    private final int limit;                 // -1 means no limit
    private final int offset;                // 0 means no offset

    public ExecSort(ExecNode input, List<String> sortColumns,
                    List<Boolean> ascending, int limit, int offset) {
        this.input = input;
        this.sortColumns = sortColumns;
        this.ascending = ascending;
        this.limit = limit;
        this.offset = offset;
    }

    public ExecSort(StreamInput in) throws IOException {
        this.input = ExecNode.readNode(in);
        this.sortColumns = in.readStringList();
        int count = in.readVInt();
        Boolean[] ascArray = new Boolean[count];
        for (int i = 0; i < count; i++) {
            ascArray[i] = in.readBoolean();
        }
        this.ascending = List.of(ascArray);
        this.limit = in.readInt();
        this.offset = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExecNode.writeNode(out, input);
        out.writeStringCollection(sortColumns);
        out.writeVInt(ascending.size());
        for (Boolean asc : ascending) {
            out.writeBoolean(asc);
        }
        out.writeInt(limit);
        out.writeInt(offset);
    }

    @Override
    public NodeType getType() {
        return NodeType.SORT;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.singletonList(input);
    }

    public ExecNode getInput() {
        return input;
    }

    public List<String> getSortColumns() {
        return sortColumns;
    }

    public List<Boolean> getAscending() {
        return ascending;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public boolean hasLimit() {
        return limit > 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExecSort[");
        for (int i = 0; i < sortColumns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(sortColumns.get(i));
            sb.append(ascending.get(i) ? " ASC" : " DESC");
        }
        if (limit > 0) {
            sb.append(", LIMIT ").append(limit);
        }
        if (offset > 0) {
            sb.append(", OFFSET ").append(offset);
        }
        sb.append("]");
        return sb.toString();
    }
}

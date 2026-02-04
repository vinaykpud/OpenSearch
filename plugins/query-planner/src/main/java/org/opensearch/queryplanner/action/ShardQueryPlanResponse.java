/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from executing a query plan fragment on a shard.
 */
public class ShardQueryPlanResponse extends ActionResponse {

    private ShardId shardId;
    private List<String> columnNames;
    private List<Object[]> rows;
    private String errorMessage;

    public ShardQueryPlanResponse() {}

    public ShardQueryPlanResponse(ShardId shardId, List<String> columnNames, List<Object[]> rows) {
        this.shardId = shardId;
        this.columnNames = columnNames;
        this.rows = rows;
        this.errorMessage = null;
    }

    public ShardQueryPlanResponse(ShardId shardId, String errorMessage) {
        this.shardId = shardId;
        this.columnNames = List.of();
        this.rows = List.of();
        this.errorMessage = errorMessage;
    }

    public ShardQueryPlanResponse(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.columnNames = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        int colCount = columnNames.size();
        for (int i = 0; i < rowCount; i++) {
            Object[] row = new Object[colCount];
            for (int j = 0; j < colCount; j++) {
                row[j] = in.readGenericValue();
            }
            rows.add(row);
        }
        this.errorMessage = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeStringCollection(columnNames);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            for (Object value : row) {
                out.writeGenericValue(value);
            }
        }
        out.writeOptionalString(errorMessage);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean hasError() {
        return errorMessage != null;
    }

    public int getRowCount() {
        return rows.size();
    }
}

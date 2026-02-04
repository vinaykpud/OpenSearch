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
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for SQL query execution containing rows and column metadata.
 */
public class QuerySqlResponse extends ActionResponse implements ToXContentObject {

    private final List<Row> rows;
    private final String[] columns;
    private final long tookMillis;

    public QuerySqlResponse(List<Row> rows, String[] columns, long tookMillis) {
        this.rows = rows;
        this.columns = columns;
        this.tookMillis = tookMillis;
    }

    public QuerySqlResponse(StreamInput in) throws IOException {
        super(in);
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            this.rows.add(new Row(in));
        }
        this.columns = in.readStringArray();
        this.tookMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rows.size());
        for (Row row : rows) {
            row.writeTo(out);
        }
        out.writeStringArray(columns);
        out.writeVLong(tookMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("took", tookMillis);

        builder.startArray("columns");
        for (String col : columns) {
            builder.value(col);
        }
        builder.endArray();

        builder.startArray("rows");
        for (Row row : rows) {
            builder.startArray();
            for (Object value : row.values) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();

        builder.field("total", rows.size());
        builder.endObject();
        return builder;
    }

    public List<Row> getRows() {
        return rows;
    }

    public String[] getColumns() {
        return columns;
    }

    public long getTookMillis() {
        return tookMillis;
    }

    /**
     * A single row of data.
     */
    public static class Row {
        private final Object[] values;

        public Row(Object[] values) {
            this.values = values;
        }

        public Row(StreamInput in) throws IOException {
            int len = in.readVInt();
            this.values = new Object[len];
            for (int i = 0; i < len; i++) {
                this.values[i] = in.readGenericValue();
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(values.length);
            for (Object v : values) {
                out.writeGenericValue(v);
            }
        }

        public Object[] getValues() {
            return values;
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.action;

import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for an optimized query execution.
 *
 * Currently returns a simple placeholder response. Will be enhanced in later phases to include:
 * - Query results (hits, aggregations)
 * - Execution statistics (time, documents processed)
 * - Physical plan information (for debugging)
 * - Optimization details (rules applied, cost estimates)
 */
public class QSearchResponse extends ActionResponse implements StatusToXContentObject {

    private final String message;
    private final String logicalPlan;
    private final String indexName;
    private final long tookInMillis;

    /**
     * Constructs a new QSearchResponse.
     *
     * @param message the response message
     * @param logicalPlan the Calcite logical plan string
     * @param indexName the target index name
     * @param tookInMillis the execution time in milliseconds
     */
    public QSearchResponse(String message, String logicalPlan, String indexName, long tookInMillis) {
        this.message = message;
        this.logicalPlan = logicalPlan;
        this.indexName = indexName;
        this.tookInMillis = tookInMillis;
    }

    /**
     * Constructs a new QSearchResponse from a stream.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public QSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.message = in.readString();
        this.logicalPlan = in.readString();
        this.indexName = in.readString();
        this.tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(message);
        out.writeString(logicalPlan);
        out.writeString(indexName);
        out.writeVLong(tookInMillis);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("message", message);
        builder.field("logicalPlan", logicalPlan);
        builder.field("indexName", indexName);
        builder.field("took", tookInMillis);
        builder.endObject();
        return builder;
    }

    /**
     * Gets the response message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the logical plan string.
     */
    public String getLogicalPlan() {
        return logicalPlan;
    }

    /**
     * Gets the index name.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Gets the execution time in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }
}

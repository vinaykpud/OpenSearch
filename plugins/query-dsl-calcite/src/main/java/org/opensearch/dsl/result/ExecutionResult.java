/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.aggregation.AggregationMetadata;

import java.util.List;

/**
 * Result from executing a single {@link QueryPlans.QueryPlan}.
 * Contains the tabular results, field names, and optional aggregation metadata.
 */
public final class ExecutionResult {

    private final QueryPlans.Type type;
    private final Object[][] rows;
    private final List<String> fieldNames;
    private final AggregationMetadata aggregationMetadata;

    /**
     * Creates an execution result with aggregation metadata.
     *
     * @param type                the type identifying what part of the response this result populates
     * @param rows                the tabular result data
     * @param fieldNames          the field names corresponding to columns in the rows
     * @param aggregationMetadata the aggregation metadata (null for HITS results)
     */
    public ExecutionResult(QueryPlans.Type type, Object[][] rows, List<String> fieldNames,
            AggregationMetadata aggregationMetadata) {
        this.type = type;
        this.rows = rows;
        this.fieldNames = List.copyOf(fieldNames);
        this.aggregationMetadata = aggregationMetadata;
    }

    /**
     * Creates an execution result without aggregation metadata.
     *
     * @param type       the type identifying what part of the response this result populates
     * @param rows       the tabular result data
     * @param fieldNames the field names corresponding to columns in the rows
     */
    public ExecutionResult(QueryPlans.Type type, Object[][] rows, List<String> fieldNames) {
        this(type, rows, fieldNames, null);
    }

    /** Returns the type of this execution result. */
    public QueryPlans.Type getType() {
        return type;
    }

    /** Returns the tabular result rows. */
    public Object[][] getRows() {
        return rows;
    }

    /** Returns the field names corresponding to the columns in the result rows. */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /** Returns the aggregation metadata, or null for HITS results. */
    public AggregationMetadata getAggregationMetadata() {
        return aggregationMetadata;
    }
}

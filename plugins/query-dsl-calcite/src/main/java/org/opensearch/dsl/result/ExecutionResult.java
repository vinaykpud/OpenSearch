/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.QueryPlans;

import java.util.List;

/**
 * Result from executing a single {@link QueryPlans.QueryPlan}.
 * Contains the tabular results and the type identifying what part of the
 * SearchResponse this result populates.
 */
public final class ExecutionResult {

    private final QueryPlans.Type type;
    private final Object[][] rows;
    private final List<String> fieldNames;

    /**
     * Creates an execution result.
     *
     * @param type       the type identifying what part of the response this result populates
     * @param rows       the tabular result data
     * @param fieldNames the field names corresponding to columns in the rows
     */
    public ExecutionResult(QueryPlans.Type type, Object[][] rows, List<String> fieldNames) {
        this.type = type;
        this.rows = rows;
        this.fieldNames = List.copyOf(fieldNames);
    }

    /**
     * Returns the type of this execution result.
     *
     * @return the type
     */
    public QueryPlans.Type getType() {
        return type;
    }

    /**
     * Returns the tabular result rows.
     *
     * @return a two-dimensional array of result data
     */
    public Object[][] getRows() {
        return rows;
    }

    /**
     * Returns the field names corresponding to the columns in the result rows.
     *
     * @return an unmodifiable list of field names
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }
}

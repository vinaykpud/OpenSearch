/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.ExecutionPath;

import java.util.List;

/**
 * Result from executing a single {@link ExecutionPath}.
 * Contains the tabular results and the role identifying what part of the
 * SearchResponse this result populates.
 */
public final class ExecutionResult {

    private final ExecutionPath.PathRole role;
    private final Object[][] rows;
    private final List<String> fieldNames;

    /**
     * Creates an execution result.
     *
     * @param role       the path role identifying what part of the response this result populates
     * @param rows       the tabular result data
     * @param fieldNames the field names corresponding to columns in the rows
     */
    public ExecutionResult(ExecutionPath.PathRole role, Object[][] rows, List<String> fieldNames) {
        this.role = role;
        this.rows = rows;
        this.fieldNames = List.copyOf(fieldNames);
    }

    /**
     * Returns the role of this execution result.
     *
     * @return the path role
     */
    public ExecutionPath.PathRole getRole() {
        return role;
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

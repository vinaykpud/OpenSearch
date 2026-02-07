/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

import java.util.Objects;

/**
 * Result of query execution.
 *
 * <p>Contains:
 * <ul>
 *   <li>Execution statistics (time, documents, rows)</li>
 *   <li>Success/failure status</li>
 *   <li>Result data (placeholder for now)</li>
 * </ul>
 *
 * <p>In future phases, this will contain actual result data
 * (Arrow batches, JSON, etc.). For now, it focuses on execution
 * statistics and status.
 */
public class QueryResult {

    private final ExecutionStatistics statistics;
    private final boolean success;
    private final String message;

    /**
     * Constructs a new QueryResult.
     *
     * @param statistics the execution statistics
     * @param success whether execution succeeded
     * @param message result message
     */
    public QueryResult(ExecutionStatistics statistics, boolean success, String message) {
        this.statistics = Objects.requireNonNull(statistics, "statistics cannot be null");
        this.success = success;
        this.message = message;
    }

    /**
     * Gets the execution statistics.
     *
     * @return the execution statistics
     */
    public ExecutionStatistics getStatistics() {
        return statistics;
    }

    /**
     * Checks if execution succeeded.
     *
     * @return true if successful, false otherwise
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Gets the result message.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format(
            "QueryResult[success=%b, message=%s, statistics=%s]",
            success,
            message,
            statistics
        );
    }
}

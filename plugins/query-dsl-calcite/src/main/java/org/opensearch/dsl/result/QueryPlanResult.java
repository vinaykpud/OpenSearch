/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.ExecutionPath;
import org.opensearch.dsl.QueryPlan;

import java.util.List;
import java.util.Optional;

/**
 * Aggregate result from executing all paths of a {@link QueryPlan}.
 * Each {@link ExecutionResult} is tagged with its role so the response builder
 * can assemble the correct parts of the SearchResponse.
 */
public final class QueryPlanResult {

    private final List<ExecutionResult> results;

    /**
     * Creates a query plan result from the given list of execution results.
     *
     * @param results the execution results, one per executed path
     */
    public QueryPlanResult(List<ExecutionResult> results) {
        this.results = List.copyOf(results);
    }

    /**
     * Returns all execution results.
     *
     * @return an unmodifiable list of execution results
     */
    public List<ExecutionResult> getAllResults() {
        return results;
    }

    /**
     * Returns the first execution result matching the given role, if any.
     *
     * @param role the path role to look up
     * @return an optional containing the matching result, or empty if none found
     */
    public Optional<ExecutionResult> getResult(ExecutionPath.PathRole role) {
        return results.stream()
            .filter(r -> r.getRole() == role)
            .findFirst();
    }
}

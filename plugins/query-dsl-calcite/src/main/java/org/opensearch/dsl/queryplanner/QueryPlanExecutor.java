/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.opensearch.dsl.QueryPlan;
import org.opensearch.dsl.result.QueryPlanResult;

/**
 * Executes a {@link QueryPlan} containing one or more execution paths
 * and returns a {@link QueryPlanResult} with role-tagged results.
 */
public interface QueryPlanExecutor {
    /**
     * Executes the given query plan and returns the combined results.
     *
     * @param plan the query plan to execute
     * @return the aggregated results from all execution paths
     * @throws Exception if execution fails
     */
    QueryPlanResult execute(QueryPlan plan) throws Exception;
}

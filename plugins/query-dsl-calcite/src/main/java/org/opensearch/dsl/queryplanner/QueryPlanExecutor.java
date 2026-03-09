/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.result.QueryPlanResult;

/**
 * Executes a {@link QueryPlans} containing one or more query plans
 * and returns a {@link QueryPlanResult} with type-tagged results.
 */
public interface QueryPlanExecutor {
    /**
     * Executes the given query plans and returns the combined results.
     *
     * @param plans the query plans to execute
     * @return the aggregated results from all query plans
     * @throws Exception if execution fails
     */
    QueryPlanResult execute(QueryPlans plans) throws Exception;
}

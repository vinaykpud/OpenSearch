/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.QueryPlanResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes {@link QueryPlans} by delegating each query plan's RelNode to a
 * {@link RelNodeExecutor}. All plans are dispatched in parallel, then
 * results are collected by blocking on each future.
 */
public class DefaultQueryPlanExecutor implements QueryPlanExecutor {

    private final RelNodeExecutor relNodeExecutor;

    /**
     * Creates a new executor that delegates RelNode execution to the given adapter.
     *
     * @param relNodeExecutor the adapter for executing individual RelNodes
     */
    public DefaultQueryPlanExecutor(RelNodeExecutor relNodeExecutor) {
        this.relNodeExecutor = relNodeExecutor;
    }

    @Override
    public QueryPlanResult execute(QueryPlans plans) throws Exception {
        List<QueryPlans.QueryPlan> queryPlans = plans.getAll();

        // Phase 1: Dispatch all plans in parallel
        List<PlainActionFuture<Object[][]>> futures = new ArrayList<>(queryPlans.size());
        for (QueryPlans.QueryPlan plan : queryPlans) {
            PlainActionFuture<Object[][]> future = PlainActionFuture.newFuture();
            relNodeExecutor.execute(plan.relNode(), future);
            futures.add(future);
        }

        // Phase 2: Wait for all results
        List<ExecutionResult> results = new ArrayList<>(queryPlans.size());
        for (int i = 0; i < queryPlans.size(); i++) {
            Object[][] rows = futures.get(i).actionGet();
            List<String> fieldNames = queryPlans.get(i).relNode().getRowType().getFieldNames();
            results.add(new ExecutionResult(queryPlans.get(i).type(), rows, fieldNames));
        }
        return new QueryPlanResult(results);
    }
}

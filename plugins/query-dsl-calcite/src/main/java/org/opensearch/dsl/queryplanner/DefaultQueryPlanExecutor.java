/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.dsl.*;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.QueryPlanResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes a {@link QueryPlan} by delegating each path's RelNode to a
 * {@link RelNodeExecutor}. All paths are dispatched in parallel, then
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
    public QueryPlanResult execute(QueryPlan plan) throws Exception {
        List<ExecutionPath> paths = plan.getAllPaths();

        // Phase 1: Dispatch all paths in parallel
        List<PlainActionFuture<Object[][]>> futures = new ArrayList<>(paths.size());
        for (ExecutionPath path : paths) {
            PlainActionFuture<Object[][]> future = PlainActionFuture.newFuture();
            relNodeExecutor.execute(path.getRelNode(), future);
            futures.add(future);
        }

        // Phase 2: Wait for all results
        List<ExecutionResult> results = new ArrayList<>(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            Object[][] rows = futures.get(i).actionGet();
            List<String> fieldNames = paths.get(i).getRelNode().getRowType().getFieldNames();
            results.add(new ExecutionResult(paths.get(i).getRole(), rows, fieldNames));
        }
        return new QueryPlanResult(results);
    }
}

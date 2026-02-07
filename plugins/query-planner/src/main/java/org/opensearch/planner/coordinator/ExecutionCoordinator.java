/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

import org.opensearch.planner.splitter.SplitPlan;

/**
 * Coordinates execution of split plans across multiple execution engines.
 *
 * <p>The execution coordinator is responsible for:
 * <ul>
 *   <li>Determining execution order using topological sort</li>
 *   <li>Executing segments in dependency order</li>
 *   <li>Managing data flow between segments</li>
 *   <li>Collecting execution statistics</li>
 *   <li>Handling execution errors</li>
 * </ul>
 *
 * <p><b>Execution Flow:</b>
 * <pre>
 * 1. Topologically sort segments by dependencies
 * 2. Execute root segments (no dependencies) first
 * 3. Pass results to dependent segments
 * 4. Continue until all segments complete
 * 5. Return final results with statistics
 * </pre>
 *
 * <p><b>Example:</b>
 * <pre>
 * ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
 * ExecutionContext context = ExecutionContext.builder()
 *     .withIndexShard(indexShard)
 *     .build();
 * QueryResult result = coordinator.execute(splitPlan, context);
 * </pre>
 */
public interface ExecutionCoordinator {

    /**
     * Executes a split plan and returns the query results.
     *
     * <p>This method:
     * <ol>
     *   <li>Determines execution order using topological sort</li>
     *   <li>Executes each segment in order</li>
     *   <li>Passes intermediate results between segments</li>
     *   <li>Collects execution statistics</li>
     *   <li>Returns final results</li>
     * </ol>
     *
     * @param splitPlan the split plan to execute
     * @param context the execution context with access to OpenSearch infrastructure
     * @return the query results with statistics
     * @throws CoordinationException if execution fails
     */
    QueryResult execute(SplitPlan splitPlan, ExecutionContext context) throws CoordinationException;
}

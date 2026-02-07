/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Execution coordination for split query plans.
 *
 * <p>This package provides the infrastructure for coordinating execution
 * across multiple execution engines (Lucene and DataFusion).
 *
 * <p><b>Key Components:</b>
 * <ul>
 *   <li>{@link org.opensearch.planner.coordinator.ExecutionCoordinator} - Orchestrates segment execution</li>
 *   <li>{@link org.opensearch.planner.coordinator.ExecutionContext} - Provides access to OpenSearch infrastructure</li>
 *   <li>{@link org.opensearch.planner.coordinator.QueryResult} - Contains execution results and statistics</li>
 *   <li>{@link org.opensearch.planner.coordinator.ExecutionStatistics} - Tracks execution metrics</li>
 * </ul>
 *
 * <p><b>Execution Flow:</b>
 * <pre>
 * 1. Coordinator receives split plan
 * 2. Topologically sorts segments by dependencies
 * 3. Executes segments in order:
 *    - Lucene segments execute first (no dependencies)
 *    - DataFusion segments execute after (depend on Lucene)
 * 4. Collects statistics for each segment
 * 5. Returns final results with statistics
 * </pre>
 *
 * <p><b>Example Usage:</b>
 * <pre>
 * ExecutionCoordinator coordinator = new DefaultExecutionCoordinator();
 * ExecutionContext context = ExecutionContext.builder()
 *     .withIndexShard(indexShard)
 *     .build();
 * QueryResult result = coordinator.execute(splitPlan, context);
 * System.out.println("Execution time: " + result.getStatistics().getTotalTimeMillis() + "ms");
 * </pre>
 */
package org.opensearch.planner.coordinator;

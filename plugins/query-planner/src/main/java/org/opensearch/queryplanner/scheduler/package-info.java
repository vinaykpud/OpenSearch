/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Query scheduling and execution components.
 *
 * <h2>Key Interfaces:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.scheduler.QueryScheduler} - Schedules and
 *       executes ExecNode plans, returning Arrow VectorSchemaRoot results.</li>
 * </ul>
 *
 * <h2>Key Classes:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.scheduler.DefaultQueryScheduler} - Default
 *       scheduler implementation that fragments plans and executes on shards.</li>
 *   <li>{@link org.opensearch.queryplanner.scheduler.SchedulerContext} - Execution context
 *       with allocator, timeout, etc.</li>
 * </ul>
 *
 * <h2>Execution Flow:</h2>
 * <pre>
 * ExecNode plan
 *     │
 *     ▼
 * QueryScheduler.schedule()
 *     │  - Builds stages at Exchange boundaries (via StageBuilder)
 *     │  - Dispatches LEAF stages to shards
 *     │  - Collects partial results
 *     │  - Executes ROOT stage on coordinator
 *     ▼
 * CompletableFuture&lt;VectorSchemaRoot&gt; result
 * </pre>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * QueryScheduler scheduler = new DefaultQueryScheduler(
 *     transportService, clusterService, indicesService);
 *
 * scheduler.schedule(execNode, context)
 *     .thenAccept(result -> {
 *         processResult(result);
 *         result.close();
 *     });
 * }</pre>
 *
 * <h2>Distributed Execution:</h2>
 * <pre>
 * Input plan:
 *   ExecAggregate(FINAL)
 *     └── ExecExchange(GATHER)
 *           └── ExecAggregate(PARTIAL)
 *                 └── ExecScan
 *
 * Execution:
 *   1. Build stages at Exchange boundary
 *   2. Dispatch LEAF stage to shards
 *   3. Collect partial results
 *   4. Execute ROOT stage on coordinator
 *   5. Return final result
 * </pre>
 */
package org.opensearch.queryplanner.scheduler;

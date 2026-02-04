/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.scheduler;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.util.concurrent.CompletableFuture;

/**
 * Scheduler for distributed query execution.
 *
 * <p>Takes an ExecNode plan, splits it into stages, dispatches to shards,
 * and returns results.
 */
public interface QueryScheduler {

    /**
     * Schedule and execute a query plan.
     *
     * @param plan the physical execution plan
     * @param context execution context with allocator, timeout, etc.
     * @return future containing query results
     */
    CompletableFuture<VectorSchemaRoot> schedule(ExecNode plan, SchedulerContext context);
}

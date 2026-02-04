/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.coordinator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;

import java.util.concurrent.CompletableFuture;

/**
 * Coordinator that orchestrates query planning and execution.
 */
public interface QueryCoordinator {

    /**
     * Execute a logical plan and return results as Arrow VectorSchemaRoot.
     *
     * @param logicalPlan the logical plan to execute
     * @return future containing VectorSchemaRoot with query results
     */
    CompletableFuture<VectorSchemaRoot> execute(RelNode logicalPlan);
}

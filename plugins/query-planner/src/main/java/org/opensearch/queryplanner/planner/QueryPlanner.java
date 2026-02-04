/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.planner;

import org.apache.calcite.rel.RelNode;

/**
 * Query planner that applies logical optimizations to a plan.
 */
public interface QueryPlanner {

    /**
     * Optimize a logical plan.
     *
     * @param logicalPlan the unoptimized logical plan
     * @return optimized logical plan
     */
    RelNode optimize(RelNode logicalPlan);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.optimizer;

import org.apache.calcite.rel.RelNode;

/**
 * Interface for query optimization.
 * 
 * Implementations apply optimization rules to transform logical plans
 * into more efficient equivalent plans.
 */
public interface QueryOptimizer {
    
    /**
     * Optimizes a logical plan using optimization rules.
     *
     * @param logicalPlan The input logical plan (Calcite RelNode)
     * @param config Configuration for optimization (rule sets, planner type)
     * @return An optimized logical plan
     * @throws OptimizationException if optimization fails
     */
    RelNode optimize(RelNode logicalPlan, OptimizationConfig config) throws OptimizationException;
    
    /**
     * Optimizes a logical plan using default configuration.
     *
     * @param logicalPlan The input logical plan (Calcite RelNode)
     * @return An optimized logical plan
     * @throws OptimizationException if optimization fails
     */
    default RelNode optimize(RelNode logicalPlan) throws OptimizationException {
        return optimize(logicalPlan, OptimizationConfig.defaultConfig());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import org.apache.calcite.rel.RelNode;

/**
 * Converts optimized logical plans into physical execution plans.
 *
 * <p>The physical planner performs the following steps:
 * <ol>
 *   <li>Traverses the optimized logical plan (post-order)</li>
 *   <li>For each logical operator, creates corresponding physical operator(s)</li>
 *   <li>Assigns execution engine based on capability detection</li>
 *   <li>Inserts data transfer operators at engine boundaries</li>
 *   <li>Validates the physical plan for correctness</li>
 * </ol>
 *
 * <p>Example:
 * <pre>
 * // Logical plan: Aggregate(Filter(TableScan))
 * RelNode logicalPlan = ...;
 * 
 * // Convert to physical plan
 * PhysicalPlanner planner = new DefaultPhysicalPlanner(capabilityDetector);
 * PhysicalPlan physicalPlan = planner.generatePhysicalPlan(logicalPlan, context);
 * 
 * // Physical plan: HashAggregate[DATAFUSION] → Transfer → Filter[LUCENE] → IndexScan[LUCENE]
 * </pre>
 */
public interface PhysicalPlanner {

    /**
     * Generates a physical plan from an optimized logical plan.
     *
     * @param optimizedPlan the optimized logical plan from Calcite
     * @param planningContext context with metadata (schema, statistics)
     * @return a physical execution plan with engine assignments
     * @throws PlanningException if physical planning fails
     */
    PhysicalPlan generatePhysicalPlan(RelNode optimizedPlan, PlanningContext planningContext) throws PlanningException;

    /**
     * Generates a physical plan with default planning context.
     *
     * @param optimizedPlan the optimized logical plan from Calcite
     * @return a physical execution plan with engine assignments
     * @throws PlanningException if physical planning fails
     */
    default PhysicalPlan generatePhysicalPlan(RelNode optimizedPlan) throws PlanningException {
        return generatePhysicalPlan(optimizedPlan, PlanningContext.createDefault());
    }
}

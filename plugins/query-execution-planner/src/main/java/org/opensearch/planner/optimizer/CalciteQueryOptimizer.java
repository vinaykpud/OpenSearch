/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.optimizer;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Calcite-based query optimizer implementation.
 * 
 * Uses Calcite's HepPlanner (heuristic planner) to apply optimization rules
 * to logical query plans.
 */
public class CalciteQueryOptimizer implements QueryOptimizer {
    
    private static final Logger logger = LogManager.getLogger(CalciteQueryOptimizer.class);
    
    /**
     * Creates a new CalciteQueryOptimizer.
     */
    public CalciteQueryOptimizer() {
        // Default constructor
    }
    
    @Override
    public RelNode optimize(RelNode logicalPlan, OptimizationConfig config) throws OptimizationException {
        if (logicalPlan == null) {
            throw new OptimizationException("Logical plan cannot be null");
        }
        
        if (config == null) {
            throw new OptimizationException("Optimization config cannot be null");
        }
        
        logger.debug("Starting optimization with {} rules", config.getRules().size());
        
        try {
            RelNode optimizedPlan;
            
            if (config.getPlannerType() == OptimizationConfig.PlannerType.HEP) {
                optimizedPlan = optimizeWithHepPlanner(logicalPlan, config);
            } else {
                // VolcanoPlanner not implemented yet
                throw new OptimizationException(
                    "VolcanoPlanner is not implemented yet. Only HepPlanner is currently supported.",
                    logicalPlan
                );
            }
            
            logger.debug("Optimization completed successfully");
            return optimizedPlan;
            
        } catch (Exception e) {
            logger.error("Optimization failed", e);
            throw new OptimizationException(
                "Failed to optimize query plan: " + e.getMessage(),
                e,
                logicalPlan
            );
        }
    }
    
    /**
     * Optimizes the plan using Calcite's HepPlanner (heuristic planner).
     * 
     * HepPlanner applies rules in a specified order until no more rules can be applied
     * or the maximum number of iterations is reached.
     */
    private RelNode optimizeWithHepPlanner(RelNode logicalPlan, OptimizationConfig config) {
        // Build HEP program with the specified rules
        HepProgramBuilder programBuilder = HepProgram.builder();
        
        // Set match order - ARBITRARY is fastest, DEPTH_FIRST is more thorough
        programBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);
        
        // Add all optimization rules
        for (RelOptRule rule : config.getRules()) {
            programBuilder.addRuleInstance(rule);
        }
        
        HepProgram program = programBuilder.build();
        
        // Create HEP planner with the program
        HepPlanner planner = new HepPlanner(program);
        
        // Set the logical plan as the root
        planner.setRoot(logicalPlan);
        
        // Execute optimization
        RelNode optimizedPlan = planner.findBestExp();
        
        // Log optimization results
        if (logger.isDebugEnabled()) {
            logger.debug("Original plan:\n{}", org.apache.calcite.plan.RelOptUtil.toString(logicalPlan));
            logger.debug("Optimized plan:\n{}", org.apache.calcite.plan.RelOptUtil.toString(optimizedPlan));
        }
        
        return optimizedPlan;
    }
}

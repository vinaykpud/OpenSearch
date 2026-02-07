/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.optimizer;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for query optimization.
 * 
 * Specifies which optimization rules to apply and how to apply them.
 */
public class OptimizationConfig {
    
    private final List<RelOptRule> rules;
    private final PlannerType plannerType;
    private final int maxIterations;
    
    /**
     * Type of planner to use for optimization.
     */
    public enum PlannerType {
        /**
         * Heuristic planner - applies rules in order (fast, may not find optimal plan).
         */
        HEP,
        
        /**
         * Cost-based planner - uses dynamic programming (slower, finds optimal plan).
         */
        VOLCANO
    }
    
    private OptimizationConfig(List<RelOptRule> rules, PlannerType plannerType, int maxIterations) {
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
        this.plannerType = plannerType;
        this.maxIterations = maxIterations;
    }
    
    /**
     * Returns the list of optimization rules to apply.
     * @return unmodifiable list of rules
     */
    public List<RelOptRule> getRules() {
        return rules;
    }
    
    /**
     * Returns the planner type to use.
     * @return the planner type (HEP or VOLCANO)
     */
    public PlannerType getPlannerType() {
        return plannerType;
    }
    
    /**
     * Returns the maximum number of optimization iterations.
     * @return the max iterations
     */
    public int getMaxIterations() {
        return maxIterations;
    }
    
    /**
     * Creates a default configuration with standard optimization rules.
     * Uses HepPlanner for simplicity and speed.
     */
    public static OptimizationConfig defaultConfig() {
        return builder()
            .withPlannerType(PlannerType.HEP)
            .withStandardRules()
            .withMaxIterations(100)
            .build();
    }
    
    /**
     * Creates a builder for custom configuration.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder for OptimizationConfig.
     */
    public static class Builder {
        private final List<RelOptRule> rules = new ArrayList<>();
        private PlannerType plannerType = PlannerType.HEP;
        private int maxIterations = 100;
        
        /**
         * Creates a new Builder.
         */
        public Builder() {
            // Default constructor
        }
        
        /**
         * Adds a single optimization rule.
         * @param rule the rule to add
         * @return this builder
         */
        public Builder withRule(RelOptRule rule) {
            this.rules.add(rule);
            return this;
        }
        
        /**
         * Adds multiple optimization rules.
         * @param rules the rules to add
         * @return this builder
         */
        public Builder withRules(List<RelOptRule> rules) {
            this.rules.addAll(rules);
            return this;
        }
        
        /**
         * Adds standard optimization rules:
         * - Filter pushdown
         * - Projection pruning
         * - Join optimization
         * - Aggregate optimization
         */
        public Builder withStandardRules() {
            // Filter optimization rules
            rules.add(CoreRules.FILTER_PROJECT_TRANSPOSE);
            rules.add(CoreRules.FILTER_MERGE);
            rules.add(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
            rules.add(CoreRules.FILTER_INTO_JOIN);
            
            // Projection optimization rules
            rules.add(CoreRules.PROJECT_MERGE);
            rules.add(CoreRules.PROJECT_REMOVE);
            rules.add(CoreRules.PROJECT_JOIN_TRANSPOSE);
            rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE);
            
            // Join optimization rules
            rules.add(CoreRules.JOIN_COMMUTE);
            rules.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
            
            // Aggregate optimization rules
            rules.add(CoreRules.AGGREGATE_PROJECT_MERGE);
            rules.add(CoreRules.AGGREGATE_REMOVE);
            
            return this;
        }
        
        /**
         * Sets the planner type (HEP or VOLCANO).
         * @param plannerType the planner type
         * @return this builder
         */
        public Builder withPlannerType(PlannerType plannerType) {
            this.plannerType = plannerType;
            return this;
        }
        
        /**
         * Sets the maximum number of optimization iterations.
         * @param maxIterations the max iterations
         * @return this builder
         */
        public Builder withMaxIterations(int maxIterations) {
            this.maxIterations = maxIterations;
            return this;
        }
        
        /**
         * Builds the configuration.
         */
        public OptimizationConfig build() {
            if (rules.isEmpty()) {
                throw new IllegalStateException("At least one optimization rule must be specified");
            }
            return new OptimizationConfig(rules, plannerType, maxIterations);
        }
    }
}

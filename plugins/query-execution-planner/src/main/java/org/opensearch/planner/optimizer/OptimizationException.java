/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.optimizer;

/**
 * Exception thrown when query optimization fails.
 */
public class OptimizationException extends Exception {
    
    private final transient org.apache.calcite.rel.RelNode originalPlan;
    
    /**
     * Creates a new OptimizationException with a message.
     * @param message the error message
     */
    public OptimizationException(String message) {
        super(message);
        this.originalPlan = null;
    }
    
    /**
     * Creates a new OptimizationException with a message and cause.
     * @param message the error message
     * @param cause the underlying cause
     */
    public OptimizationException(String message, Throwable cause) {
        super(message, cause);
        this.originalPlan = null;
    }
    
    /**
     * Creates a new OptimizationException with a message and original plan.
     * @param message the error message
     * @param originalPlan the original plan before optimization failed
     */
    public OptimizationException(String message, org.apache.calcite.rel.RelNode originalPlan) {
        super(message);
        this.originalPlan = originalPlan;
    }
    
    /**
     * Creates a new OptimizationException with a message, cause, and original plan.
     * @param message the error message
     * @param cause the underlying cause
     * @param originalPlan the original plan before optimization failed
     */
    public OptimizationException(String message, Throwable cause, org.apache.calcite.rel.RelNode originalPlan) {
        super(message, cause);
        this.originalPlan = originalPlan;
    }
    
    /**
     * Returns the original plan before optimization failed.
     * May be null if the plan was not available.
     */
    public org.apache.calcite.rel.RelNode getOriginalPlan() {
        return originalPlan;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

/**
 * Exception thrown when physical planning fails.
 *
 * <p>This exception is thrown when the physical planner encounters an error
 * during the conversion of a logical plan to a physical plan. Common causes:
 * <ul>
 *   <li>Unsupported logical operator type</li>
 *   <li>Invalid operator configuration</li>
 *   <li>Schema mismatch between operators</li>
 *   <li>Missing required metadata</li>
 * </ul>
 */
public class PlanningException extends Exception {

    /**
     * Constructs a new PlanningException with the specified message.
     *
     * @param message the error message
     */
    public PlanningException(String message) {
        super(message);
    }

    /**
     * Constructs a new PlanningException with the specified message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public PlanningException(String message, Throwable cause) {
        super(message, cause);
    }
}

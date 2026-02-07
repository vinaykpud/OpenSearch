/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.splitter;

/**
 * Exception thrown when a physical plan cannot be split into execution segments.
 *
 * <p>This exception indicates an error during plan splitting, such as:
 * <ul>
 *   <li>Invalid operator tree structure</li>
 *   <li>Circular dependencies detected</li>
 *   <li>Unsupported operator combinations</li>
 *   <li>Missing required operators</li>
 * </ul>
 */
public class SplitException extends RuntimeException {

    /**
     * Constructs a new SplitException with the specified message.
     *
     * @param message the error message
     */
    public SplitException(String message) {
        super(message);
    }

    /**
     * Constructs a new SplitException with the specified message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public SplitException(String message, Throwable cause) {
        super(message, cause);
    }
}

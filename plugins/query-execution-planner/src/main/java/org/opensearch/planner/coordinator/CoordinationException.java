/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

/**
 * Exception thrown when execution coordination fails.
 *
 * <p>This exception wraps underlying execution errors and provides
 * context about which segment failed and why.
 */
public class CoordinationException extends Exception {

    private final String segmentId;

    /**
     * Constructs a new CoordinationException.
     *
     * @param message the error message
     */
    public CoordinationException(String message) {
        super(message);
        this.segmentId = null;
    }

    /**
     * Constructs a new CoordinationException with a cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public CoordinationException(String message, Throwable cause) {
        super(message, cause);
        this.segmentId = null;
    }

    /**
     * Constructs a new CoordinationException with segment context.
     *
     * @param message the error message
     * @param segmentId the ID of the segment that failed
     * @param cause the underlying cause
     */
    public CoordinationException(String message, String segmentId, Throwable cause) {
        super(message, cause);
        this.segmentId = segmentId;
    }

    /**
     * Gets the ID of the segment that failed.
     *
     * @return the segment ID, or null if not applicable
     */
    public String getSegmentId() {
        return segmentId;
    }
}

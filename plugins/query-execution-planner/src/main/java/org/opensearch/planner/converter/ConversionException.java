/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.converter;

/**
 * Exception thrown when DSL to Calcite conversion fails.
 *
 * This exception wraps underlying conversion errors and provides
 * meaningful error messages for debugging.
 */
public class ConversionException extends Exception {

    /**
     * Constructs a new ConversionException with the specified detail message.
     *
     * @param message The detail message
     */
    public ConversionException(String message) {
        super(message);
    }

    /**
     * Constructs a new ConversionException with the specified detail message and cause.
     *
     * @param message The detail message
     * @param cause The cause of the exception
     */
    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.exception;

/**
 * Exception thrown when DSL to Calcite conversion fails.
 * 
 * This exception is used to signal errors during the conversion process,
 * such as unsupported query types or invalid field references.
 */
public class ConversionException extends Exception {

    private final String queryType;
    private final String reason;

    /**
     * Constructor for ConversionException.
     * 
     * @param queryType The type of query that failed to convert (may be null)
     * @param reason The reason for the conversion failure
     */
    public ConversionException(String queryType, String reason) {
        super(buildMessage(queryType, reason));
        this.queryType = queryType;
        this.reason = reason;
    }

    /**
     * Constructor for ConversionException with cause.
     * 
     * @param queryType The type of query that failed to convert (may be null)
     * @param reason The reason for the conversion failure
     * @param cause The underlying cause of the exception
     */
    public ConversionException(String queryType, String reason, Throwable cause) {
        super(buildMessage(queryType, reason), cause);
        this.queryType = queryType;
        this.reason = reason;
    }

    /**
     * Creates a ConversionException for unsupported query types.
     * 
     * @param type The unsupported query type
     * @return A ConversionException instance
     */
    public static ConversionException unsupportedQuery(String type) {
        return new ConversionException(type, "Query type not supported");
    }

    /**
     * Creates a ConversionException for invalid field references.
     * 
     * @param field The invalid field name
     * @return A ConversionException instance
     */
    public static ConversionException invalidField(String field) {
        return new ConversionException(null, "Field '" + field + "' not found in schema");
    }

    /**
     * Gets the query type that failed to convert.
     * 
     * @return The query type, or null if not applicable
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * Gets the reason for the conversion failure.
     * 
     * @return The reason string
     */
    public String getReason() {
        return reason;
    }

    /**
     * Builds the exception message.
     * 
     * @param queryType The query type
     * @param reason The reason
     * @return The formatted message
     */
    private static String buildMessage(String queryType, String reason) {
        if (queryType != null) {
            return "Conversion failed for query type '" + queryType + "': " + reason;
        }
        return "Conversion failed: " + reason;
    }
}

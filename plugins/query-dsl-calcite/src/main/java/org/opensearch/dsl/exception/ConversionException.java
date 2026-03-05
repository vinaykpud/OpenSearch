/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.exception;

/**
 * Exception thrown when DSL to Calcite conversion fails.
 * 
 * This exception is used to signal errors during the conversion process,
 * such as unsupported stages or invalid field references.
 */
public class ConversionException extends Exception {

    private final String stage;
    private final String reason;

    /**
     * Constructor for ConversionException.
     * 
     * @param stage The type of query that failed to convert (may be null)
     * @param reason The reason for the conversion failure
     */
    public ConversionException(String stage, String reason) {
        super(buildMessage(stage, reason));
        this.stage = stage;
        this.reason = reason;
    }

    /**
     * Constructor for ConversionException with cause.
     * 
     * @param stage The type of query that failed to convert (may be null)
     * @param reason The reason for the conversion failure
     * @param cause The underlying cause of the exception
     */
    public ConversionException(String stage, String reason, Throwable cause) {
        super(buildMessage(stage, reason), cause);
        this.stage = stage;
        this.reason = reason;
    }

    /**
     * Creates a ConversionException for unsupported stages.
     * 
     * @param type The unsupported stage
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
     * Creates a ConversionException for unsupported RelNode types.
     *
     * @param relNodeType The unsupported RelNode type name
     * @return A ConversionException instance
     */
    public static ConversionException unsupportedRelNode(String relNodeType) {
        return new ConversionException(relNodeType,
            relNodeType + " is not supported by downstream DistQuerySystem");
    }

    /**
     * Creates a ConversionException for unsupported SQL operators.
     *
     * @param operatorName The unsupported operator name
     * @return A ConversionException instance
     */
    public static ConversionException unsupportedOperator(String operatorName) {
        return new ConversionException("operator",
            "Operator '" + operatorName + "' is not supported by downstream DistQuerySystem");
    }

    /**
     * Creates a ConversionException for unsupported aggregate functions.
     *
     * @param functionName The unsupported aggregate function name
     * @return A ConversionException instance
     */
    public static ConversionException unsupportedAggFunction(String functionName) {
        return new ConversionException("aggregation",
            "Aggregate function '" + functionName + "' is not supported by downstream DistQuerySystem");
    }

    /**
     * Gets the stage that failed to convert.
     * 
     * @return The stage, or null if not applicable
     */
    public String getStage() {
        return stage;
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
     * @param stage The stage
     * @param reason The reason
     * @return The formatted message
     */
    private static String buildMessage(String stage, String reason) {
        if (stage != null) {
            return "Conversion failed for stage '" + stage + "': " + reason;
        }
        return "Conversion failed: " + reason;
    }
}

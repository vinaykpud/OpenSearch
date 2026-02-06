/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import java.util.List;

/**
 * Physical operator for limiting the number of rows returned.
 *
 * <p>This operator limits its output to a specified number of rows,
 * optionally skipping a number of rows first (offset).
 *
 * <p><b>Execution:</b> Can be executed by either engine.
 *
 * <p><b>Example:</b>
 * <pre>
 * LimitOperator limit = new LimitOperator(
 *     10,  // Return 10 rows
 *     0,   // Skip 0 rows (no offset)
 *     ExecutionEngine.DATAFUSION,
 *     Collections.singletonList(sortOperator),
 *     Arrays.asList("name", "price")
 * );
 * </pre>
 */
public class LimitOperator extends PhysicalOperator {

    private final long limit;
    private final long offset;

    /**
     * Constructs a new LimitOperator.
     *
     * @param limit the maximum number of rows to return
     * @param offset the number of rows to skip
     * @param executionEngine the engine to execute this limit
     * @param children the input operators
     * @param outputSchema the output schema (same as input)
     */
    public LimitOperator(
        long limit,
        long offset,
        ExecutionEngine executionEngine,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.LIMIT, executionEngine, children, outputSchema);
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Gets the limit (maximum number of rows).
     *
     * @return the limit
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Gets the offset (number of rows to skip).
     *
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitLimit(this);
    }

    @Override
    public String toString() {
        return String.format(
            "LimitOperator[limit=%d, offset=%d, engine=%s, schema=%s]",
            limit,
            offset,
            getExecutionEngine(),
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        LimitOperator that = (LimitOperator) obj;
        return limit == that.limit && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), limit, offset);
    }
}

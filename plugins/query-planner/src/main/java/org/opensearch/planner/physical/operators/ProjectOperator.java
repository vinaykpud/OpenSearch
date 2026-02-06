/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

import java.util.List;

/**
 * Physical operator for projecting (selecting) specific columns/fields.
 *
 * <p>This operator selects a subset of columns from its input and produces
 * only those columns in the output.
 *
 * <p><b>Execution:</b> Can be executed by either engine depending on context.
 *
 * <p><b>Example:</b>
 * <pre>
 * ProjectOperator project = new ProjectOperator(
 *     ExecutionEngine.LUCENE,
 *     Collections.singletonList(scanOperator),
 *     Arrays.asList("name", "price") // Only these fields
 * );
 * </pre>
 */
public class ProjectOperator extends PhysicalOperator {

    /**
     * Constructs a new ProjectOperator.
     *
     * @param executionEngine the engine to execute this projection
     * @param children the input operators
     * @param outputSchema the fields to project (output schema)
     */
    public ProjectOperator(
        ExecutionEngine executionEngine,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.PROJECT, executionEngine, children, outputSchema);
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitProject(this);
    }

    @Override
    public String toString() {
        return String.format(
            "ProjectOperator[engine=%s, schema=%s]",
            getExecutionEngine(),
            getOutputSchema()
        );
    }
}

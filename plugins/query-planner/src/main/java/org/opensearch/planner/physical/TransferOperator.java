/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical;

import java.util.List;
import java.util.Objects;

/**
 * Physical operator for transferring data between execution engines.
 *
 * <p>This operator converts data from one engine's format to another.
 * For example, converting Lucene documents to Arrow RecordBatch for
 * DataFusion processing.
 *
 * <p><b>Execution:</b> Hybrid - involves both engines.
 *
 * <p><b>Common Transfers:</b>
 * <ul>
 *   <li>Lucene → DataFusion: Convert documents to Arrow format</li>
 *   <li>DataFusion → Lucene: Convert Arrow to documents (rare)</li>
 * </ul>
 *
 * <p><b>Example:</b>
 * <pre>
 * TransferOperator transfer = new TransferOperator(
 *     ExecutionEngine.LUCENE,      // From Lucene
 *     ExecutionEngine.DATAFUSION,  // To DataFusion
 *     Collections.singletonList(filterOperator),
 *     Arrays.asList("name", "category", "price")
 * );
 * </pre>
 */
public class TransferOperator extends PhysicalOperator {

    private final ExecutionEngine fromEngine;
    private final ExecutionEngine toEngine;

    /**
     * Constructs a new TransferOperator.
     *
     * @param fromEngine the source execution engine
     * @param toEngine the destination execution engine
     * @param children the input operators
     * @param outputSchema the output schema (same as input)
     */
    public TransferOperator(
        ExecutionEngine fromEngine,
        ExecutionEngine toEngine,
        List<PhysicalOperator> children,
        List<String> outputSchema
    ) {
        super(OperatorType.TRANSFER, ExecutionEngine.HYBRID, children, outputSchema);
        this.fromEngine = Objects.requireNonNull(fromEngine, "fromEngine cannot be null");
        this.toEngine = Objects.requireNonNull(toEngine, "toEngine cannot be null");
        if (fromEngine == toEngine) {
            throw new IllegalArgumentException("fromEngine and toEngine must be different");
        }
    }

    /**
     * Gets the source execution engine.
     *
     * @return the from engine
     */
    public ExecutionEngine getFromEngine() {
        return fromEngine;
    }

    /**
     * Gets the destination execution engine.
     *
     * @return the to engine
     */
    public ExecutionEngine getToEngine() {
        return toEngine;
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitTransfer(this);
    }

    @Override
    public String toString() {
        return String.format(
            "TransferOperator[from=%s, to=%s, schema=%s]",
            fromEngine,
            toEngine,
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        TransferOperator that = (TransferOperator) obj;
        return fromEngine == that.fromEngine && toEngine == that.toEngine;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fromEngine, toEngine);
    }
}

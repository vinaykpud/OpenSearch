/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.physical.operators;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Physical operator for scanning a Lucene index.
 *
 * <p>This operator reads documents from a Lucene index and produces a stream
 * of documents. It is typically a leaf node in the operator tree.
 *
 * <p><b>Execution:</b> Always executed by the Lucene engine.
 *
 * <p><b>Example:</b>
 * <pre>
 * IndexScanOperator scan = new IndexScanOperator(
 *     "products",
 *     Arrays.asList("name", "category", "price")
 * );
 * </pre>
 */
public class IndexScanOperator extends PhysicalOperator {

    private final String indexName;

    /**
     * Constructs a new IndexScanOperator.
     *
     * @param indexName the name of the index to scan
     * @param outputSchema the fields to retrieve from the index
     */
    public IndexScanOperator(String indexName, List<String> outputSchema) {
        super(
            OperatorType.INDEX_SCAN,
            ExecutionEngine.LUCENE,
            Collections.emptyList(), // Leaf node - no children
            outputSchema
        );
        this.indexName = Objects.requireNonNull(indexName, "indexName cannot be null");
    }

    /**
     * Gets the name of the index to scan.
     *
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    @Override
    public <T> T accept(PhysicalOperatorVisitor<T> visitor) {
        return visitor.visitIndexScan(this);
    }

    @Override
    public String toString() {
        return String.format(
            "IndexScanOperator[index=%s, schema=%s]",
            indexName,
            getOutputSchema()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        IndexScanOperator that = (IndexScanOperator) obj;
        return Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexName);
    }
}

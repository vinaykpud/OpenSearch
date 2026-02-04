/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.operator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.queryplanner.physical.operator.Operator;

import java.util.List;

/**
 * Scan operator - reads data from an index.
 *
 * <p>The scan operator is the leaf node in the operator tree. It reads
 * data from an OpenSearch index using doc values. This is not used in the native engine flow - Features:
 * <ul>
 *   <li>Column projection - only reads requested columns</li>
 *   <li>Filter pushdown - applies Lucene query at scan time</li>
 *   <li>Batched output - returns data in Arrow VectorSchemaRoot batches</li>
 * </ul>
 */
public class ScanOperator implements Operator {

    private final String indexName;
    private final List<String> columns;
    private final String filter;
    private final DataProvider dataProvider;

    private BufferAllocator allocator;
    private boolean exhausted = false;


    public ScanOperator(String indexName, List<String> columns, String filter, DataProvider dataProvider) {
        this.indexName = indexName;
        this.columns = columns;
        this.filter = filter;
        this.dataProvider = dataProvider;
    }

    @Override
    public void open(BufferAllocator allocator) {
        this.allocator = allocator;
        this.exhausted = false;
        if (dataProvider != null) {
            dataProvider.open(indexName, columns, filter);
        }
    }

    @Override
    public VectorSchemaRoot next() {
        if (exhausted) {
            return null;
        }

        if (dataProvider == null) {
            exhausted = true;
            return null;
        }

        VectorSchemaRoot batch = dataProvider.nextBatch(allocator);
        if (batch == null || batch.getRowCount() == 0) {
            exhausted = true;
            if (batch != null) {
                batch.close();
            }
            return null;
        }

        return batch;
    }

    @Override
    public void close() {
        if (dataProvider != null) {
            dataProvider.close();
        }
    }

    /**
     * Interface for providing data to scan operator.
     * Implementations can read from Lucene, mock data, etc.
     * I think we can use this to feed DISI from lucene if needed
     */
    public interface DataProvider {
        /**
         * Open the data provider for reading.
         *
         * @param indexName The index to read from
         * @param columns The columns to project
         * @param filter Optional filter expression (may be null)
         */
        void open(String indexName, List<String> columns, String filter);

        /**
         * Get the next batch of data.
         *
         * @param allocator Arrow buffer allocator
         * @return VectorSchemaRoot batch, or null if exhausted
         */
        VectorSchemaRoot nextBatch(BufferAllocator allocator);

        /**
         * Close and release resources.
         */
        void close();
    }
}

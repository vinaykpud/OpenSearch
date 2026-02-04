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

import java.util.Iterator;
import java.util.List;

/**
 * Operator that returns pre-loaded VectorSchemaRoots.
 * Used to feed gathered shard results into coordinator-side operators.
 *
 * <p>This operator is used in the ROOT stage to provide the concatenated
 * partial results from LEAF stages as input to FINAL operators (e.g., FINAL aggregate).
 */
public class GatheredDataOperator implements Operator {

    private final List<VectorSchemaRoot> batches;
    private Iterator<VectorSchemaRoot> iterator;

    /**
     * Create operator with pre-loaded batches.
     *
     * @param batches The VectorSchemaRoots to return, one per next() call
     */
    public GatheredDataOperator(List<VectorSchemaRoot> batches) {
        this.batches = batches;
    }

    @Override
    public void open(BufferAllocator allocator) {
        this.iterator = batches.iterator();
    }

    @Override
    public VectorSchemaRoot next() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        // Don't close the batches here - they're managed by the caller
        this.iterator = null;
    }
}

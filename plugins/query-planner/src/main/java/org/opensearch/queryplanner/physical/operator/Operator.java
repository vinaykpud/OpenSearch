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

/**
 * Volcano-style pull-based operator.
 *
 * <p>Operators form a tree where each operator pulls batches from its children.
 * Each call to {@link #next()} returns a batch of rows as a VectorSchemaRoot.
 *
 * <p>Usage:
 * <pre>
 * operator.open(allocator);
 * try {
 *     VectorSchemaRoot batch;
 *     while ((batch = operator.next()) != null) {
 *         // process batch
 *         batch.close();
 *     }
 * } finally {
 *     operator.close();
 * }
 * </pre>
 */
public interface Operator {

    /**
     * Open the operator and its children.
     *
     * @param allocator Arrow allocator for creating vectors
     */
    void open(BufferAllocator allocator);

    /**
     * Get the next batch of rows.
     *
     * @return batch of rows, or null if no more data
     */
    VectorSchemaRoot next();

    /**
     * Close the operator and release resources.
     */
    void close();
}

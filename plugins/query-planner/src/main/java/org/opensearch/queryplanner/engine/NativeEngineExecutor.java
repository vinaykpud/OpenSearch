/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.engine;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.operator.ScanOperator;

/**
 * Interface for native engine execution.
 *
 * <p>Implementations of this interface execute an ExecNode subtree and return
 * results as Arrow VectorSchemaRoot.
 *
 * <h2>Implementations:</h2>
 * <ul>
 *   <li>{@link org.opensearch.queryplanner.engine.datafusion.DataFusionExecutor} - Executes via DataFusion JNI</li>
 * </ul>
 */
public interface NativeEngineExecutor {

    /**
     * Execute an ExecNode subtree and return results.
     *
     * @param subPlan The ExecNode tree to execute
     * @param dataProvider Provider for scan data (may be null for engine-native scans)
     * @param allocator Arrow memory allocator
     * @return Query results as Arrow VectorSchemaRoot
     */
    VectorSchemaRoot execute(ExecNode subPlan, ScanOperator.DataProvider dataProvider, BufferAllocator allocator);
}

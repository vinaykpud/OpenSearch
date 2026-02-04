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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.engine.NativeEngineExecutor;
import org.opensearch.queryplanner.physical.exec.ExecNode;

/**
 * Operator that executes an ExecNode subtree via a native engine.
 *
 * <p>The EngineOperator bridges ExecEngine nodes to NativeEngineExecutor
 * implementations. It extracts the subPlan from ExecEngine and delegates
 * execution to the configured executor.
 *
 * <h2>Execution Flow:</h2>
 * <pre>
 * EngineOperator.open(allocator)
 *     │
 *     ▼
 * EngineOperator.next()
 *     │
 *     ▼
 * NativeEngineExecutor.execute(subPlan, dataProvider, allocator)
 *     │
 *     ▼
 * VectorSchemaRoot (result)
 * </pre>
 */
public class EngineOperator implements Operator {

    private static final Logger logger = LogManager.getLogger(EngineOperator.class);

    private final ExecNode subPlan;
    private final NativeEngineExecutor executor;
    private final ScanOperator.DataProvider dataProvider;

    private BufferAllocator allocator;
    private VectorSchemaRoot result;
    private boolean executed;

    /**
     * Create an engine operator.
     *
     * @param subPlan The ExecNode subtree to execute
     * @param executor The native engine executor
     * @param dataProvider Provider for scan data
     */
    public EngineOperator(ExecNode subPlan, NativeEngineExecutor executor,
                          ScanOperator.DataProvider dataProvider) {
        this.subPlan = subPlan;
        this.executor = executor;
        this.dataProvider = dataProvider;
        this.executed = false;
    }

    @Override
    public void open(BufferAllocator allocator) {
        this.allocator = allocator;
        this.executed = false;
        this.result = null;
        logger.info("EngineOperator opened for subPlan: {}", subPlan.getType());
    }

    @Override
    public VectorSchemaRoot next() {
        if (executed) {
            // Engine executes the entire subPlan at once, so only one batch
            return null;
        }

        executed = true;
        logger.info("Executing subPlan via engine executor");

        // Execute the entire subPlan via the native engine
        result = executor.execute(subPlan, dataProvider, allocator);

        logger.info("Engine executor produced {} rows", result != null ? result.getRowCount() : 0);
        return result;
    }

    @Override
    public void close() {
        // Note: result ownership is transferred to caller, so we don't close it here
        logger.info("EngineOperator closed");
    }
}

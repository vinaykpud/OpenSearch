/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.queryplanner.engine.NativeEngineExecutor;
import org.opensearch.queryplanner.engine.datafusion.DataFusionBridge;
import org.opensearch.queryplanner.engine.datafusion.DataFusionExecutor;
import org.opensearch.queryplanner.physical.exec.ExecAggregate;
import org.opensearch.queryplanner.physical.exec.ExecEngine;
import org.opensearch.queryplanner.physical.exec.ExecExchange;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.ExecScan;

/**
 * Factory for creating Operators from ExecNodes.
 *
 * <p>In the native engine execution path, most operators (filter, project, sort, limit)
 * are handled by DataFusion. This factory primarily creates:
 * <ul>
 *   <li>ScanOperator - for reading data from Lucene</li>
 *   <li>AggregateOperator - for FINAL aggregation at coordinator (for now - we want to send this to df)</li>
 *   <li>EngineOperator - for delegating to DataFusion</li>
 * </ul>
 */
public class OperatorFactory {

    private static final Logger logger = LogManager.getLogger(OperatorFactory.class);

    private final ScanOperator.DataProvider dataProvider;
    private final NativeEngineExecutor engineExecutor;

    public OperatorFactory(ScanOperator.DataProvider dataProvider) {
        this(dataProvider, createDefaultExecutor());
    }

    /**
     * Create the default engine executor (DataFusion).
     */
    private static NativeEngineExecutor createDefaultExecutor() {
        if (!DataFusionBridge.isAvailable()) {
            String error = DataFusionBridge.getLoadError();
            throw new IllegalStateException("DataFusion native library not available: " +
                (error != null ? error : "library not found"));
        }
        logger.info("Using DataFusion native engine executor");
        return new DataFusionExecutor();
    }

    public OperatorFactory(ScanOperator.DataProvider dataProvider, NativeEngineExecutor engineExecutor) {
        this.dataProvider = dataProvider;
        this.engineExecutor = engineExecutor;
    }

    /**
     * Build an operator tree from an ExecNode tree.
     */
    public Operator create(ExecNode node) {
        switch (node.getType()) {
            case SCAN:
                return new ScanOperator(
                    ((ExecScan) node).getIndexName(),
                    ((ExecScan) node).getColumns(),
                    ((ExecScan) node).getFilter(),
                    dataProvider);
            case AGGREGATE:
                ExecAggregate agg = (ExecAggregate) node;
                Operator child = create(agg.getInput());
                return new AggregateOperator(child, agg.getGroupBy(), agg.getAggregates(), agg.getMode());
            case EXCHANGE:
                // Exchange is handled at coordinator level
                // For shard execution, just execute the input
                return create(((ExecExchange) node).getInput());
            case ENGINE:
                // Engine operator executes the entire subPlan via DataFusion
                return new EngineOperator(((ExecEngine) node).getSubPlan(), engineExecutor, dataProvider);
            case FILTER:
            case PROJECT:
            case SORT:
            case LIMIT:
                // These are handled by DataFusion inside ExecEngine
                throw new UnsupportedOperationException(
                    node.getType() + " should be executed by DataFusion, not as standalone operator");
            default:
                throw new UnsupportedOperationException("Unknown node type: " + node.getType());
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.scheduler;

import org.opensearch.queryplanner.physical.exec.ExecNode;

/**
 * Represents a stage in distributed query execution.
 *
 * <p>A query plan is split at Exchange boundaries into stages:
 * <ul>
 *   <li>LEAF stages run on data nodes (shards)</li>
 *   <li>ROOT stage runs on the coordinator</li>
 * </ul>
 *
 * <pre>
 * Example: SELECT category, SUM(price) FROM orders GROUP BY category
 *
 * ROOT stage (coordinator):
 *   ExecAggregate(FINAL)
 *     └── [results from LEAF stage]
 *
 * LEAF stage (each shard):
 *   ExecAggregate(PARTIAL)
 *     └── ExecScan
 * </pre>
 */
public class QueryStage {

    /**
     * Type of stage determining where it executes.
     */
    public enum StageType {
        /** Runs on data nodes (shards). */
        LEAF,
        /** Runs on coordinator after gathering shard results. */
        ROOT
    }

    private final int stageId;
    private final StageType type;
    private final ExecNode planFragment;

    public QueryStage(int stageId, StageType type, ExecNode planFragment) {
        this.stageId = stageId;
        this.type = type;
        this.planFragment = planFragment;
    }

    public int getStageId() {
        return stageId;
    }

    public StageType getType() {
        return type;
    }

    public ExecNode getPlanFragment() {
        return planFragment;
    }

    @Override
    public String toString() {
        return "QueryStage{id=" + stageId + ", type=" + type + ", plan=" + planFragment + "}";
    }
}

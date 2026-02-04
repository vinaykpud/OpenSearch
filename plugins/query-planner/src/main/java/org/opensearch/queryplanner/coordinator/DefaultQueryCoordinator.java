/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.coordinator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;
import org.opensearch.queryplanner.optimizer.PhysicalOptimizer;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.exec.RelToExecConverter;
import org.opensearch.queryplanner.planner.DefaultQueryPlanner;
import org.opensearch.queryplanner.planner.QueryPlanner;
import org.opensearch.queryplanner.scheduler.DefaultQueryScheduler;
import org.opensearch.queryplanner.scheduler.QueryScheduler;
import org.opensearch.queryplanner.scheduler.SchedulerContext;
import org.opensearch.transport.TransportService;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of QueryCoordinator.
 */
public class DefaultQueryCoordinator implements QueryCoordinator {
    private static final Logger logger = LogManager.getLogger(DefaultQueryCoordinator.class);

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final QueryPlanner planner;
    private final PhysicalOptimizer physicalOptimizer;
    private final RelToExecConverter relToExecConverter;
    private final QueryScheduler scheduler;
    private final BufferAllocator allocator;

    public DefaultQueryCoordinator(
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            BufferAllocator allocator) {
        this.allocator = allocator;
        this.planner = new DefaultQueryPlanner();
        this.physicalOptimizer = new PhysicalOptimizer(true);
        this.relToExecConverter = new RelToExecConverter();
        this.scheduler = new DefaultQueryScheduler(transportService, clusterService, indicesService);
    }

    @Override
    public CompletableFuture<VectorSchemaRoot> execute(RelNode logicalPlan) {
        RelNode optimized = planner.optimize(logicalPlan);
        logger.info("Output logical plan:\n{}", safeExplain(optimized));
        RelNode physical = physicalOptimizer.optimize(optimized);
        logger.info("Output physical plan:\n{}", safeExplain(physical));
        ExecNode execNode = relToExecConverter.convert(physical);
        logger.info("Exec - {}", ExecNode.explain(execNode));
        SchedulerContext context = new SchedulerContext(DEFAULT_TIMEOUT, allocator);
        return scheduler.schedule(execNode, context);
    }

    /**
     * Safely get plan string representation, falling back to toString if explain() fails.
     */
    private String safeExplain(RelNode plan) {
        try {
            return RelOptUtil.dumpPlan("", plan, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        } catch (Exception e) {
            logger.trace("dumpPlan() failed, using toString: {}", e.getMessage());
            return plan.toString();
        }
    }
}

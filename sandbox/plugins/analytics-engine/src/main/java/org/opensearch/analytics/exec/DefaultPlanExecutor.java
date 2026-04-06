/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.cluster.service.ClusterService;

/**
 * Coordinator-level plan executor. Plans the query via the capability-aware planner
 * and delegates execution to the {@link Scheduler} for distributed shard dispatch.
 *
 * <p>Created manually in {@code createComponents()} (NOT Guice-injected) so that
 * {@code AnalyticsEngineService.setInstance()} can be called with the executor
 * instance during plugin initialization.
 *
 * @opensearch.internal
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;

    public DefaultPlanExecutor(CapabilityRegistry capabilityRegistry, ClusterService clusterService, Scheduler scheduler) {
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.scheduler = scheduler;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        QueryDAG dag = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        logger.info("[DefaultPlanExecutor] QueryDAG:\n{}", dag);
        PlanWalker walker = new PlanWalker(dag, clusterService.state());
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);
        return future.actionGet();  // single blocking point — will become async when API changes
    }
}

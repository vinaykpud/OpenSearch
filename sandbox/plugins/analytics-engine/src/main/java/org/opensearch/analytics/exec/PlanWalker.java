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
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-side fully async DAG traversal. One per query. Created by
 * {@code DefaultPlanExecutor}, passed to {@code Scheduler}. Never blocks —
 * takes an {@code ActionListener} and signals completion via callbacks.
 *
 * <p>The {@code stageSinks} map stores each stage's accumulated results.
 * For multi-stage DAGs, parent stages will read from child stage sinks
 * (wired in prompt 2). For now, coordinator-only stages produce empty results.
 *
 * @opensearch.internal
 */
public class PlanWalker {
    private static final Logger logger = LogManager.getLogger(PlanWalker.class);

    private final QueryDAG dag;
    private final ClusterState clusterState;
    private final Map<Integer, ExchangeSink> stageSinks = new HashMap<>();

    public PlanWalker(QueryDAG dag, ClusterState clusterState) {
        this.dag = dag;
        this.clusterState = clusterState;
    }

    public String getQueryId() {
        return dag.queryId();
    }

    /**
     * Walks the DAG bottom-up, dispatching tasks for each stage asynchronously.
     * Calls the listener with the root stage's result when all stages complete.
     */
    public void walk(TaskSubmitter submitter, ActionListener<Iterable<Object[]>> listener) {
        walkStage(
            dag.rootStage(),
            submitter,
            ActionListener.wrap(v -> listener.onResponse(stageSinks.get(dag.rootStage().getStageId()).readResult()), listener::onFailure)
        );
    }

    /**
     * Walks a single stage: first walks all children sequentially (bottom-up),
     * then executes this stage. Completion signals the stageListener.
     */
    private void walkStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> stageListener) {
        walkChildrenSequentially(
            stage.getChildStages(),
            0,
            submitter,
            ActionListener.wrap(v -> executeStage(stage, submitter, stageListener), stageListener::onFailure)
        );
    }

    /**
     * Walks child stages one at a time, left to right. When all children are done,
     * calls the listener. Each child's completion triggers the next child.
     */
    private void walkChildrenSequentially(List<Stage> children, int index, TaskSubmitter submitter, ActionListener<Void> listener) {
        if (index >= children.size()) {
            listener.onResponse(null);
            return;
        }
        walkStage(
            children.get(index),
            submitter,
            ActionListener.wrap(v -> walkChildrenSequentially(children, index + 1, submitter, listener), listener::onFailure)
        );
    }

    /**
     * Executes a single stage: resolves targets, sets up sink, dispatches all tasks.
     * When all shard responses arrive, closes the sink and signals completion.
     */
    private void executeStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> stageListener) {
        StagePlan plan = stage.getPlanAlternatives().get(0);
        String backendId = extractResolvedBackend(plan.resolvedFragment());

        List<TargetShard> targets = resolveTargets(stage);

        SimpleExchangeSink sink = new SimpleExchangeSink();
        stageSinks.put(stage.getStageId(), sink);

        if (targets.isEmpty()) {
            // Coordinator-only stage (e.g., final aggregate with StageInputScan).
            // Coordinator stage execution (consuming child sinks) is handled in prompt 2.
            // For now, multi-stage DAGs return empty results from the root stage.
            sink.close();
            stageListener.onResponse(null);
            return;
        }

        // Submit ALL tasks at once — Scheduler gates per-node concurrency via PendingExecutions
        AtomicInteger remaining = new AtomicInteger(targets.size());
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (TargetShard target : targets) {
            FragmentExecutionRequest request = new FragmentExecutionRequest(
                dag.queryId(),
                stage.getStageId(),
                UUID.randomUUID().toString(),
                target.shardId(),
                backendId,
                null,
                plan.resolvedFragment()
            );

            submitter.submit(request, target.node(), new ActionListener<>() {
                @Override
                public void onResponse(FragmentExecutionResponse response) {
                    synchronized (sink) {
                        sink.feed(response);
                    }
                    checkStageComplete();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    logger.error("Shard execution failed for stage {}: {}", stage.getStageId(), e.getMessage(), e);
                    checkStageComplete();
                }

                private void checkStageComplete() {
                    if (remaining.decrementAndGet() == 0) {
                        sink.close();
                        Exception e = failure.get();
                        if (e != null) {
                            stageListener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                        } else {
                            stageListener.onResponse(null);
                        }
                    }
                }
            });
        }
    }

    /**
     * Resolve target shards/nodes based on the stage's exchange type.
     *
     * - Leaf stage (no exchange, has TableScan): all shards of the index
     * - SINGLETON exchange: all primary shards of the stage's TableScan
     * - HASH exchange: throw UnsupportedOperationException (future)
     * - No TableScan (coordinator-only): empty target list
     */
    private List<TargetShard> resolveTargets(Stage stage) {
        ExchangeInfo exchange = stage.getExchangeInfo();

        if (exchange == null) {
            String tableName = extractTableName(stage.getPlanAlternatives().get(0).resolvedFragment());
            if (tableName != null) {
                return resolveIndexShards(tableName);
            }
            return List.of();
        }

        switch (exchange.distributionType()) {
            case SINGLETON:
                String tableName = extractTableName(stage.getPlanAlternatives().get(0).resolvedFragment());
                return resolveIndexShards(tableName);

            case HASH_DISTRIBUTED:
                throw new UnsupportedOperationException("HASH exchange not yet supported");

            default:
                throw new IllegalStateException("Unknown exchange type: " + exchange.distributionType());
        }
    }

    private List<TargetShard> resolveIndexShards(String tableName) {
        Index index = clusterState.metadata().index(tableName).getIndex();
        IndexRoutingTable routingTable = clusterState.routingTable().index(index);
        List<TargetShard> targets = new ArrayList<>();
        for (int i = 0; i < routingTable.shards().size(); i++) {
            ShardRouting primary = routingTable.shard(i).primaryShard();
            DiscoveryNode node = clusterState.nodes().get(primary.currentNodeId());
            targets.add(new TargetShard(primary.shardId(), node));
        }
        return targets;
    }

    /** Simple record for shard + node pairing. */
    record TargetShard(ShardId shardId, DiscoveryNode node) {
    }

    // ---- Helpers ----

    /** Walk the resolved fragment tree to find any OpenSearchRelNode and read its backend. */
    static String extractResolvedBackend(RelNode node) {
        if (node instanceof OpenSearchRelNode osNode) {
            return osNode.getViableBackends().get(0);
        }
        for (RelNode input : node.getInputs()) {
            String backend = extractResolvedBackend(input);
            if (backend != null) return backend;
        }
        return null;
    }

    /** Walk the fragment tree to find OpenSearchTableScan and extract the table name. */
    static String extractTableName(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) {
            List<String> qn = scan.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        return null;  // coordinator stages with StageInputScan have no TableScan
    }
}

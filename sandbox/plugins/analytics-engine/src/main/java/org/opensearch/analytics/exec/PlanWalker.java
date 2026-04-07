/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-side DAG traversal. One per query. Created by
 * {@code DefaultPlanExecutor}, passed to {@code Scheduler}. Never blocks —
 * takes an {@code ActionListener} and signals completion via callbacks.
 *
 * <p>Uses a single root sink (created from the root stage's fragment) that only
 * the direct children of the root feed into. The {@code stageOutputs} map tracks
 * what each completed stage produced — either {@code RowData} (rows fed into the
 * root sink) or {@code PartitionManifest} (metadata for shuffle target resolution).
 *
 * @opensearch.internal
 */
public class PlanWalker {
    private static final Logger logger = LogManager.getLogger(PlanWalker.class);

    // Immutable query context
    private final QueryDAG dag;
    private final ClusterService clusterService;

    // Per-query mutable state
    private final ExchangeSink rootSink;
    private final Map<Integer, StageOutput> stageOutputs = new HashMap<>();

    /** Output produced by a completed stage. */
    sealed interface StageOutput {
        /** Rows already fed into the root sink. No additional data. */
        record RowData() implements StageOutput {}

        /** Partition manifests from each shard: shardId → (partitionId → filePath). */
        record PartitionManifest(Map<ShardId, Map<Integer, String>> manifests) implements StageOutput {}
    }

    /** Shard + node pairing. */
    record TargetShard(ShardId shardId, DiscoveryNode node) {}

    public PlanWalker(QueryDAG dag, ClusterService clusterService) {
        this.dag = dag;
        this.clusterService = clusterService;
        this.rootSink = createRootSink(dag.rootStage());
    }

    public String getQueryId() {
        return dag.queryId();
    }

    /**
     * Walks the DAG bottom-up, dispatching tasks for each stage asynchronously.
     * Calls the listener with the root sink's result when all stages complete.
     */
    public void walk(TaskSubmitter submitter, ActionListener<Iterable<Object[]>> listener) {
        walkStage(dag.rootStage(), submitter, ActionListener.wrap(
            v -> listener.onResponse(rootSink.readResult()),
            listener::onFailure
        ));
    }

    /**
     * Walks a single stage: first walks all children sequentially (bottom-up),
     * then dispatches this stage. Completion signals the stageListener.
     */
    private void walkStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> stageListener) {
        walkChildrenSequentially(stage.getChildStages(), 0, submitter, ActionListener.wrap(
            v -> dispatchStage(stage, submitter, stageListener),
            stageListener::onFailure
        ));
    }

    /**
     * Walks child stages one at a time, left to right. When all children are done,
     * calls the listener. Each child's completion triggers the next child.
     */
    private void walkChildrenSequentially(List<Stage> children, int index,
                                          TaskSubmitter submitter, ActionListener<Void> listener) {
        if (index >= children.size()) {
            listener.onResponse(null);
            return;
        }
        walkStage(children.get(index), submitter, ActionListener.wrap(
            v -> walkChildrenSequentially(children, index + 1, submitter, listener),
            listener::onFailure
        ));
    }

    /**
     * Dispatches a single stage: resolves targets, submits tasks, routes responses.
     * Coordinator gather stages complete immediately. Row responses feed the sink;
     * metadata responses are collected into a manifest map.
     */
    private void dispatchStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> listener) {
        // Coordinator gather — child results already in rootSink
        if (stage.isCoordinatorGather()) {
            stageOutputs.put(stage.getStageId(), new StageOutput.RowData());
            listener.onResponse(null);
            return;
        }

        List<TargetShard> targets = resolveTargets(stage);
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        boolean collectMetadata = stage.isShuffleWrite();
        Map<ShardId, Map<Integer, String>> manifests = collectMetadata ? new ConcurrentHashMap<>() : null;

        AtomicInteger remaining = new AtomicInteger(targets.size());
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (TargetShard target : targets) {
            FragmentExecutionRequest request = new FragmentExecutionRequest(
                dag.queryId(), stage.getStageId(), UUID.randomUUID().toString(),
                target.shardId(), planAlternatives
            );

            submitter.submit(request, target.node(), new ActionListener<>() {
                @Override
                public void onResponse(FragmentExecutionResponse response) {
                    if (response.hasMetadata()) {
                        manifests.put(target.shardId(), parseManifest(response.getMetadata()));
                    } else {
                        synchronized (rootSink) {
                            rootSink.feed(response);
                        }
                    }
                    checkComplete();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    logger.error("Shard execution failed for stage {}: {}", stage.getStageId(), e.getMessage(), e);
                    checkComplete();
                }

                private void checkComplete() {
                    if (remaining.decrementAndGet() == 0) {
                        Exception e = failure.get();
                        if (e != null) {
                            listener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                        } else {
                            stageOutputs.put(stage.getStageId(), collectMetadata
                                ? new StageOutput.PartitionManifest(manifests)
                                : new StageOutput.RowData());
                            listener.onResponse(null);
                        }
                    }
                }
            });
        }
    }

    // ---- Sink creation ----

    /**
     * Creates the root sink from the root stage's fragment.
     * MVP: SimpleExchangeSink (no computation, just collects rows).
     * Future: backend-provided sink embedding the root stage's computation
     * (final aggregate, sort, filter, project) for streaming reduction.
     */
    ExchangeSink createRootSink(Stage rootStage) {
        return new SimpleExchangeSink();
    }

    // ---- Plan alternative building ----

    private List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(
                plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }

    // ---- Target resolution ----

    /**
     * Resolve target shards/nodes based on the stage's properties.
     * Uses {@code stage.getTableName()} and {@code stage.isShuffleWrite()}
     * — no RelNode tree walking needed.
     */
    List<TargetShard> resolveTargets(Stage stage) {
        if (stage.getTableName() != null) {
            return resolveIndexShards(stage.getTableName());
        }
        if (stage.isShuffleWrite()) {
            return resolveShuffleTargets(stage);
        }
        return List.of();
    }

    private List<TargetShard> resolveIndexShards(String tableName) {
        ClusterState state = clusterService.state();
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(state, new String[] { tableName }, null, null);

        List<TargetShard> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = state.nodes().get(shard.currentNodeId());
                targets.add(new TargetShard(shard.shardId(), node));
            }
        }
        return targets;
    }

    private List<TargetShard> resolveShuffleTargets(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            StageOutput childOutput = stageOutputs.get(child.getStageId());
            if (childOutput instanceof StageOutput.PartitionManifest manifest) {
                return pickShuffleTargetNodes(manifest);
            }
        }
        throw new IllegalStateException("No partition manifest found for stage " + stage.getStageId());
    }

    private List<TargetShard> pickShuffleTargetNodes(StageOutput.PartitionManifest manifest) {
        ClusterState state = clusterService.state();
        List<DiscoveryNode> sourceNodes = manifest.manifests().keySet().stream()
            .map(shardId -> state.nodes().get(
                state.routingTable().index(shardId.getIndex())
                    .shard(shardId.id()).primaryShard().currentNodeId()))
            .distinct()
            .toList();

        int numPartitions = manifest.manifests().values().iterator().next().size();

        List<TargetShard> targets = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            DiscoveryNode node = sourceNodes.get(p % sourceNodes.size());
            targets.add(new TargetShard(new ShardId(new Index("_shuffle", "_na_"), p), node));
        }
        return targets;
    }

    private Map<Integer, String> parseManifest(Map<String, String> metadata) {
        Map<Integer, String> manifest = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            manifest.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return manifest;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for shard segment replication operation. This transport action does not actually
 * perform segment replication, it only reports on metrics/stats of segment replication event (both active and complete).
 *
 * @opensearch.internal
 */
public class TransportSegmentReplicationStatsAction extends TransportBroadcastByNodeAction<
    SegmentReplicationStatsRequest,
    SegmentReplicationStatsResponse,
    SegmentReplicationShardStatsResponse> {

    private final SegmentReplicationTargetService targetService;
    private final IndicesService indicesService;
    private final SegmentReplicationPressureService pressureService;

    @Inject
    public TransportSegmentReplicationStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        SegmentReplicationTargetService targetService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SegmentReplicationPressureService pressureService
    ) {
        super(
            SegmentReplicationStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            SegmentReplicationStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
        this.targetService = targetService;
        this.pressureService = pressureService;
    }

    @Override
    protected SegmentReplicationShardStatsResponse readShardResult(StreamInput in) throws IOException {
        return new SegmentReplicationShardStatsResponse(in);
    }

    @Override
    protected SegmentReplicationStatsResponse newResponse(
        SegmentReplicationStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SegmentReplicationShardStatsResponse> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {

        final List<Integer> shardsToFetch = Arrays.stream(request.shards()).map(Integer::valueOf).collect(Collectors.toList());

        final Map<String, Set<SegmentReplicationShardStats>> replicaStats = responses.stream()
            .filter(Objects::nonNull)
            .filter(response -> !response.isPrimary() && response.getReplicaStats() != null)
            .filter(response -> shardsToFetch.isEmpty() || shardsToFetch.contains(response.getShardId().getId()))
            .collect(Collectors.groupingBy(
                response -> response.getShardId().getIndexName(),
                Collectors.mapping(
                    SegmentReplicationShardStatsResponse::getReplicaStats,
                    Collectors.toSet()
                )
            ));


        final Map<String, List<SegmentReplicationPerGroupStats>> segRepStats = responses.stream()
            .filter(Objects::nonNull)
            .filter(response -> !response.isPrimary() && response.getReplicaStats() != null)
            .filter(response -> shardsToFetch.isEmpty() || shardsToFetch.contains(response.getShardId().getId()))
            .collect(Collectors.groupingBy(
                response -> response.getShardId().getIndexName(),
                Collectors.mapping(
                    response -> new SegmentReplicationPerGroupStats(
                        response.getShardId(),
                        replicaStats.get(response.getShardId().getIndexName()),
                        0
                    ),
                    Collectors.toList()
                )
            ));

        return new SegmentReplicationStatsResponse(totalShards, successfulShards, failedShards, segRepStats, shardFailures);
    }

    @Override
    protected SegmentReplicationStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new SegmentReplicationStatsRequest(in);
    }

    @Override
    protected SegmentReplicationShardStatsResponse shardOperation(SegmentReplicationStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        ShardId shardId = shardRouting.shardId();

        if (indexShard.indexSettings().isSegRepEnabledOrRemoteNode() == false) {
            return null;
        }

        //StateObject exist but no checkpoint set, then treat 0, ie no update

        ReplicationCheckpoint indexReplicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
//        ReplicationCheckpoint latestReplicationCheckpointReceived = targetService.getLatestReceivedCheckpoint().get(shardId);
        SegmentReplicationState segmentReplicationState = targetService.getSegmentReplicationState(shardId);
        if(segmentReplicationState != null) {
            ReplicationCheckpoint latestReplicationCheckpointReceived = segmentReplicationState.getLatestReplicationCheckpoint();
            long checkpointsBehindCount = calculateCheckpointsBehind(indexReplicationCheckpoint, latestReplicationCheckpointReceived);
            long bytesBehindCount = calculateBytesBehind(indexReplicationCheckpoint, latestReplicationCheckpointReceived);
            long currentReplicationLag = calculateCurrentReplicationLag(shardId);
            long lastCompletedReplicationLag = request.activeOnly() ? 0 : getLastCompletedReplicationLag(shardId);

            SegmentReplicationShardStats segmentReplicationShardStats = new SegmentReplicationShardStats(
                shardRouting.allocationId().getId(),
                checkpointsBehindCount,
                bytesBehindCount,
                0,
                currentReplicationLag,
                lastCompletedReplicationLag
            );

            segmentReplicationShardStats.setCurrentReplicationState(segmentReplicationState);
            return new SegmentReplicationShardStatsResponse(shardId, shardRouting.primary(), segmentReplicationShardStats);
        } else {
            SegmentReplicationShardStats segmentReplicationShardStats = new SegmentReplicationShardStats(
                shardRouting.allocationId().getId(),
                0,
                0,
                0,
                0,
                0
            );

            segmentReplicationShardStats.setCurrentReplicationState(segmentReplicationState);
            return new SegmentReplicationShardStatsResponse(shardId, shardRouting.primary(), segmentReplicationShardStats);
        }
    }

    @Override
    protected ShardsIterator shards(ClusterState state, SegmentReplicationStatsRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SegmentReplicationStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        SegmentReplicationStatsRequest request,
        String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    private long calculateCheckpointsBehind(
        ReplicationCheckpoint indexReplicationCheckpoint,
        ReplicationCheckpoint latestReplicationCheckpointReceived
    ) {
        if (latestReplicationCheckpointReceived != null) {
            return latestReplicationCheckpointReceived.getSegmentInfosVersion() - indexReplicationCheckpoint.getSegmentInfosVersion();
        }
        return 0;
    }

    private long calculateBytesBehind(
        ReplicationCheckpoint indexReplicationCheckpoint,
        ReplicationCheckpoint latestReplicationCheckpointReceived
    ) {
        if (latestReplicationCheckpointReceived != null) {
            Store.RecoveryDiff diff = Store.segmentReplicationDiff(
                latestReplicationCheckpointReceived.getMetadataMap(),
                indexReplicationCheckpoint.getMetadataMap()
            );
            return diff.missing.stream().mapToLong(StoreFileMetadata::length).sum();
        }
        return 0;
    }

    private long calculateCurrentReplicationLag(ShardId shardId) {
        SegmentReplicationState ongoingEventSegmentReplicationState = targetService.getOngoingEventSegmentReplicationState(shardId);
        return ongoingEventSegmentReplicationState != null ? ongoingEventSegmentReplicationState.getTimer().time() : 0;
    }

    private long getLastCompletedReplicationLag(ShardId shardId) {
        SegmentReplicationState lastCompletedSegmentReplicationState = targetService.getlatestCompletedEventSegmentReplicationState(shardId);
        return lastCompletedSegmentReplicationState != null ? lastCompletedSegmentReplicationState.getTimer().time() : 0;
    }
}

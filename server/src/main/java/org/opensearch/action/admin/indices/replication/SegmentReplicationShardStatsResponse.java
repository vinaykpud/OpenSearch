/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.SegmentReplicationShardStats;

import java.io.IOException;

/**
 * Segment Replication specific response object for fetching stats from either a primary
 * or replica shard. The stats returned are different depending on primary or replica.
 *
 * @opensearch.internal
 */
public class SegmentReplicationShardStatsResponse implements Writeable {

    private final ShardId shardId;

    private final boolean isPrimary;

    private final SegmentReplicationShardStats replicaStats;

    public SegmentReplicationShardStatsResponse(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.isPrimary = in.readBoolean();
        this.replicaStats = in.readOptionalWriteable(SegmentReplicationShardStats::new);
    }

    public SegmentReplicationShardStatsResponse(ShardId shardId,
                                                boolean isPrimary,
                                                SegmentReplicationShardStats replicaStats) {
        this.shardId = shardId;
        this.isPrimary = isPrimary;
        this.replicaStats = replicaStats;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public SegmentReplicationShardStats getReplicaStats() {
        return replicaStats;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeBoolean(isPrimary);
        out.writeOptionalWriteable(replicaStats);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStatsResponse{" + "shardId=" + shardId + ", replicaStats=" + replicaStats + '}';
    }
}

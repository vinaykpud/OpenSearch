/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Transport request carrying a plan fragment to a data node for shard-level execution.
 * The {@code fragment} field is transient — set only for local dispatch and never serialized.
 */
public class FragmentExecutionRequest extends ActionRequest {

    private final String queryId;
    private final int stageId;
    private final String taskId;
    private final ShardId shardId;
    private final String backendId;
    private final byte[] fragmentBytes;   // Backend serializable plan (null for Lucene)
    private final RelNode fragment;       // transient — not serialized, for local execution only

    public FragmentExecutionRequest(
        String queryId,
        int stageId,
        String taskId,
        ShardId shardId,
        String backendId,
        byte[] fragmentBytes,
        RelNode fragment
    ) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.shardId = shardId;
        this.backendId = backendId;
        this.fragmentBytes = fragmentBytes;
        this.fragment = fragment;
    }

    /**
     * Deserialization constructor. The transient {@code fragment} field is always null
     * after deserialization — it is only set for local dispatch.
     */
    public FragmentExecutionRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readInt();
        this.taskId = in.readString();
        this.shardId = new ShardId(in);
        this.backendId = in.readString();
        byte[] bytes = in.readByteArray();
        this.fragmentBytes = (bytes.length == 0) ? null : bytes;
        this.fragment = null; // transient — not read from the wire
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeInt(stageId);
        out.writeString(taskId);
        shardId.writeTo(out);
        out.writeString(backendId);
        // Serialize null fragmentBytes as zero-length marker
        out.writeByteArray(fragmentBytes != null ? fragmentBytes : new byte[0]);
        // fragment is transient — not written to the wire
    }

    public String getQueryId() {
        return queryId;
    }

    public int getStageId() {
        return stageId;
    }

    public String getTaskId() {
        return taskId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public String getBackendId() {
        return backendId;
    }

    public byte[] getFragmentBytes() {
        return fragmentBytes;
    }

    public RelNode getFragment() {
        return fragment;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

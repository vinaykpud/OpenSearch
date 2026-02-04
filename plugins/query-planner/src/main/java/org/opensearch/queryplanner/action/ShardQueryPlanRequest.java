/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.queryplanner.physical.exec.ExecNode;

import java.io.IOException;

/**
 * Request to execute a query plan fragment on a specific shard.
 */
public class ShardQueryPlanRequest extends ActionRequest {

    private ShardId shardId;
    private ExecNode planFragment;

    public ShardQueryPlanRequest() {}

    public ShardQueryPlanRequest(ShardId shardId, ExecNode planFragment) {
        this.shardId = shardId;
        this.planFragment = planFragment;
    }

    public ShardQueryPlanRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.planFragment = ExecNode.readNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        ExecNode.writeNode(out, planFragment);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ShardId shardId() {
        return shardId;
    }

    public ExecNode getPlanFragment() {
        return planFragment;
    }
}

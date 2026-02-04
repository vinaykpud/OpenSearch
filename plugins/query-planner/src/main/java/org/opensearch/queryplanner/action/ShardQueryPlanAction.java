/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.opensearch.action.ActionType;

/**
 * Action type for shard-level query plan execution.
 */
public class ShardQueryPlanAction extends ActionType<ShardQueryPlanResponse> {

    public static final ShardQueryPlanAction INSTANCE = new ShardQueryPlanAction();
    public static final String NAME = "indices:data/read/query_plan/shard";

    private ShardQueryPlanAction() {
        super(NAME, ShardQueryPlanResponse::new);
    }
}

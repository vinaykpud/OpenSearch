/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.queryplanner.action.QuerySqlAction;
import org.opensearch.queryplanner.action.ShardQueryPlanAction;
import org.opensearch.queryplanner.action.TransportQuerySqlAction;
import org.opensearch.queryplanner.action.TransportShardQueryPlanAction;
import org.opensearch.queryplanner.action.rest.RestQuerySqlAction;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * Query planner plugin that provides SQL parsing, optimization using Apache Calcite,
 * and query execution.
 *
 * <p>Entry Points:
 * <ul>
 *   <li>REST API: POST /_plugins/query_planner/sql - Execute SQL query</li>
 *   <li>Transport Action: QuerySqlAction - For programmatic execution</li>
 * </ul>
 */
public class QueryPlannerPlugin extends Plugin implements ActionPlugin {

    public QueryPlannerPlugin() {
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(QuerySqlAction.INSTANCE, TransportQuerySqlAction.class),
            new ActionHandler<>(ShardQueryPlanAction.INSTANCE, TransportShardQueryPlanAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {

        return List.of(
            new RestQuerySqlAction()
        );
    }
}

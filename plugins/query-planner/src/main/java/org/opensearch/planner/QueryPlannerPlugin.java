/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.planner.action.QSearchAction;
import org.opensearch.planner.action.TransportQSearchAction;
import org.opensearch.planner.rest.RestQSearchAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * OpenSearch plugin for query optimization and physical planning.
 *
 * This plugin provides:
 * - Query optimization using Apache Calcite
 * - Physical plan generation with engine assignment (Lucene vs DataFusion)
 * - Hybrid execution coordinating between Lucene and DataFusion
 * - REST endpoint /_qsearch for executing optimized queries
 *
 * Dependencies:
 * - query-dsl-calcite plugin: For DSL → Calcite logical plan conversion
 * - engine-datafusion plugin: For DataFusion execution
 *
 * This is a learning exercise focused on single-node, single-shard execution.
 */
public class QueryPlannerPlugin extends Plugin implements ActionPlugin {

    /**
     * Plugin constructor.
     */
    public QueryPlannerPlugin() {
        // Plugin initialization
        // Components will be created in createComponents() method
    }

    /**
     * Returns the REST handlers for this plugin.
     *
     * Currently provides:
     * - /_qsearch endpoint for executing optimized queries
     *
     * @param settings OpenSearch settings
     * @param restController REST controller for registering handlers
     * @param clusterSettings Cluster-level settings
     * @param indexScopedSettings Index-level settings
     * @param settingsFilter Settings filter
     * @param indexNameExpressionResolver Index name resolver
     * @param nodesInCluster Supplier for discovery nodes
     * @return List of REST handlers
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Collections.singletonList(new RestQSearchAction());
    }

    /**
     * Returns the actions for this plugin.
     *
     * Actions define the transport layer operations.
     *
     * @return List of action handlers
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(new ActionHandler<>(QSearchAction.INSTANCE, TransportQSearchAction.class));
    }
}

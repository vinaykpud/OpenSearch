/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.dsl.queryplanner.DefaultQueryPlanExecutor;
import org.opensearch.dsl.queryplanner.QueryPlanExecutor;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin entry point for the OpenSearch DSL to Calcite converter.
 *
 * Converts OpenSearch DSL queries into Apache Calcite logical plans (RelNode),
 * executes them via an ActionFilter re-route (bypassing TransportSearchAction),
 * and returns a SearchResponse.
 */
public class DslLogicalPlanPlugin extends Plugin implements ActionPlugin {

    private DslLogicalPlanService converterService;
    private QueryPlanExecutor queryPlanExecutor;
    private DslRerouteFilter rerouteFilter;

    /**
     * Creates a new DSL logical plan plugin instance.
     *
     * @param settings the node-level settings
     */
    public DslLogicalPlanPlugin(Settings settings) {
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        converterService = new DslLogicalPlanService(client);
        queryPlanExecutor = new DefaultQueryPlanExecutor();
        rerouteFilter = new DslRerouteFilter(converterService, queryPlanExecutor);
        return List.of(converterService, queryPlanExecutor);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(rerouteFilter);
    }

    /**
     * Returns the converter service for direct access to QueryPlans conversion.
     * Visible for testing.
     */
    DslLogicalPlanService getConverterService() {
        return converterService;
    }

    /**
     * Returns the query plan executor.
     * Visible for testing.
     */
    QueryPlanExecutor getQueryPlanExecutor() {
        return queryPlanExecutor;
    }
}

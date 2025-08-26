/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.datafusion.action.DataFusionAction;
import org.opensearch.datafusion.action.NodesDataFusionInfoAction;
import org.opensearch.datafusion.action.TransportNodesDataFusionInfoAction;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EngineExtendPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Main plugin class for OpenSearch DataFusion integration.
 */
public class DataFusionPlugin extends Plugin implements ActionPlugin, EngineExtendPlugin {

    private DataFusionService dataFusionService;
    private final boolean isDataFusionEnabled;

    /**
     * Constructor for DataFusionPlugin.
     * @param settings The settings for the DataFusionPlugin.
     */
    public DataFusionPlugin(Settings settings) {
        // DataFusion can be disabled for integration tests or if native library is not available
        this.isDataFusionEnabled = Boolean.parseBoolean(System.getProperty("opensearch.experimental.feature.datafusion.enabled", "true"));
    }

    @Override
    public void execute(byte[] queryPlanIR) {
        LogManager.getLogger(DataFusionPlugin.class).info("Executing queryPlanIR: {}", queryPlanIR);
        LogManager.getLogger(DataFusionPlugin.class).info("Substrait plan serialized to " + queryPlanIR.length + " bytes");

        if (dataFusionService == null) {
            LogManager.getLogger(DataFusionPlugin.class).error("DataFusionService is not initialized");
            return;
        }

        try {
            // Use the default context created at service startup
            long defaultContextId = dataFusionService.getDefaultContextId();
            if (defaultContextId == 0) {
                throw new RuntimeException("No default DataFusion context available");
            }

            // Execute the Substrait query plan using the existing default context
            String result = dataFusionService.executeSubstraitQueryPlan(defaultContextId, queryPlanIR);
            LogManager.getLogger(DataFusionPlugin.class).info("Query execution result: {}", result);

        } catch (Exception exception) {
            LogManager.getLogger(DataFusionPlugin.class).error("Failed to execute Substrait query plan", exception);
        }
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(new AbstractModule() {
            @Override
            protected void configure() {
                bind(EngineExtendPlugin.class).toInstance(DataFusionPlugin.this);
            }
        });
    }

    /**
     * Creates components for the DataFusion plugin.
     * @param client The client instance.
     * @param clusterService The cluster service instance.
     * @param threadPool The thread pool instance.
     * @param resourceWatcherService The resource watcher service instance.
     * @param scriptService The script service instance.
     * @param xContentRegistry The named XContent registry.
     * @param environment The environment instance.
     * @param nodeEnvironment The node environment instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param repositoriesServiceSupplier The supplier for the repositories service.
     * @return Collection of created components
     */
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
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }

        dataFusionService = new DataFusionService();
        return Collections.singletonList(dataFusionService);
    }

    /**
     * Gets the REST handlers for the DataFusion plugin.
     * @param settings The settings for the plugin.
     * @param restController The REST controller instance.
     * @param clusterSettings The cluster settings instance.
     * @param indexScopedSettings The index scoped settings instance.
     * @param settingsFilter The settings filter instance.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param nodesInCluster The supplier for the discovery nodes.
     * @return A list of REST handlers.
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
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        return List.of(new DataFusionAction());
    }

    /**
     * Gets the list of action handlers for the DataFusion plugin.
     * @return A list of action handlers.
     */
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }
        return List.of(
            new ActionHandler<>(NodesDataFusionInfoAction.INSTANCE, TransportNodesDataFusionInfoAction.class)
        );
    }
}

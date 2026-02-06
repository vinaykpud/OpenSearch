/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite;

import org.apache.calcite.rel.RelNode;
import org.opensearch.calcite.converter.CalciteConverter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.DslConverterPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
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
 * Main plugin class for OpenSearch DSL to Calcite converter.
 *
 * This plugin provides functionality to convert OpenSearch DSL queries
 * into Apache Calcite logical plans (RelNode).
 *
 * This plugin is extensible, allowing other plugins (like query-planner)
 * to access its classes and services.
 */
public class DslCalcitePlugin extends Plugin implements DslConverterPlugin, ExtensiblePlugin {

    private CalciteConverterService converterService;

    /**
     * Constructor for DslCalcitePlugin.
     *
     * @param settings The settings for the plugin
     */
    public DslCalcitePlugin(Settings settings) {
        // Plugin initialization
    }

    /**
     * Creates components for the DSL Calcite plugin.
     *
     * @param client The client instance
     * @param clusterService The cluster service instance
     * @param threadPool The thread pool instance
     * @param resourceWatcherService The resource watcher service instance
     * @param scriptService The script service instance
     * @param xContentRegistry The named XContent registry
     * @param environment The environment instance
     * @param nodeEnvironment The node environment instance
     * @param namedWriteableRegistry The named writeable registry
     * @param indexNameExpressionResolver The index name expression resolver instance
     * @param repositoriesServiceSupplier The supplier for the repositories service
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
        // Pass client to CalciteConverterService for index mapping retrieval
        converterService = new CalciteConverterService(client);

        // No need to register - PluginsService automatically discovers plugins implementing DslConverterPlugin

        return Collections.singletonList(converterService);
    }

    /**
     * Gets the plugin settings.
     *
     * @return A list of plugin settings (empty for now)
     */
    @Override
    public List<Setting<?>> getSettings() {
        // Plugin settings can be added here if needed
        return Collections.emptyList();
    }

    /**
     * Converts an OpenSearch DSL query to a string representation.
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A string representation of the converted query
     */
    @Override
    public String convertDsl(org.opensearch.search.builder.SearchSourceBuilder source, String indexName) {
        try {
            CalciteConverter converter = converterService.getConverter();
            RelNode relNode = converter.convert(source, indexName);

            // Handle null RelNode (POC returns null for now)
            return (relNode != null) ? relNode.explain() : "null (POC - converter not yet implemented)";
        } catch (Exception e) {
            // Return full exception details for debugging
            StringBuilder sb = new StringBuilder();
            sb.append("Error: ").append(e.getClass().getSimpleName()).append(": ").append(e.getMessage());
            if (e.getCause() != null) {
                sb.append("\nCaused by: ").append(e.getCause().getClass().getSimpleName()).append(": ").append(e.getCause().getMessage());
            }
            // Add stack trace for debugging
            sb.append("\nStack trace: ");
            for (StackTraceElement element : e.getStackTrace()) {
                if (element.getClassName().startsWith("org.opensearch.calcite") || 
                    element.getClassName().startsWith("org.apache.calcite")) {
                    sb.append("\n  at ").append(element);
                }
            }
            return sb.toString();
        }
    }

    @Override
    public String getConverterName() {
        return "calcite";
    }
}

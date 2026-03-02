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
 * Plugin entry point for the OpenSearch DSL to Calcite converter.
 *
 * Converts OpenSearch DSL queries into Apache Calcite logical plans (RelNode).
 * Implements {@link DslConverterPlugin} so that {@code TransportSearchAction} can
 * discover and invoke this converter via {@code PluginsService}.
 *
 * Also implements {@link ExtensiblePlugin} to allow downstream plugins
 * (e.g. query-planner) to access converter classes and services.
 */
public class DslCalcitePlugin extends Plugin implements DslConverterPlugin, ExtensiblePlugin {

    private CalciteConverterService converterService;

    public DslCalcitePlugin(Settings settings) {
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
        converterService = new CalciteConverterService(client);
        return Collections.singletonList(converterService);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    /**
     * Converts an OpenSearch DSL query to an annotated string representation.
     * Field references are resolved to {@code $N:fieldName} format for readability.
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return An annotated string representation of the converted query
     */
    @Override
    public String convertDsl(org.opensearch.search.builder.SearchSourceBuilder source, String indexName) {
        try {
            CalciteConverter converter = converterService.getConverter();
            RelNode relNode = converter.convert(source, indexName);
            return (relNode != null) ? relNode.explain() : "null";
        } catch (Exception e) {
            return formatError(e);
        }
    }

    /** Formats an exception into a detailed error string for debugging. */
    private String formatError(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("Error: ").append(e.getClass().getSimpleName()).append(": ").append(e.getMessage());
        if (e.getCause() != null) {
            sb.append("\nCaused by: ").append(e.getCause().getClass().getSimpleName()).append(": ").append(e.getCause().getMessage());
        }
        sb.append("\nStack trace: ");
        for (StackTraceElement element : e.getStackTrace()) {
            if (element.getClassName().startsWith("org.opensearch.calcite")
                || element.getClassName().startsWith("org.apache.calcite")) {
                sb.append("\n  at ").append(element);
            }
        }
        return sb.toString();
    }

    @Override
    public String getConverterName() {
        return "calcite";
    }
}

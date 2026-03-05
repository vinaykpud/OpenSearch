/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.search.SearchResponse;
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
 * Converts OpenSearch DSL queries into Apache Calcite logical plans (RelNode),
 * executes them, and returns a SearchResponse.
 */
public class DslLogicalPlanPlugin extends Plugin implements DslConverterPlugin, ExtensiblePlugin {

    private DslLogicalPlanService converterService;
    private QueryPlanExecutor queryPlanExecutor;

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
        return List.of(converterService, queryPlanExecutor);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    @Override
    public SearchResponse convertDsl(org.opensearch.search.builder.SearchSourceBuilder source,
            String indexName) throws Exception {
        long startTime = System.currentTimeMillis();

        RelNode relNode = converterService.convert(source, indexName);
        Object[][] rows = queryPlanExecutor.execute(relNode);

        List<String> fieldNames = relNode.getRowType().getFieldNames();
        long tookInMillis = System.currentTimeMillis() - startTime;

        return SearchResponseBuilder.build(rows, fieldNames, tookInMillis);
    }
}

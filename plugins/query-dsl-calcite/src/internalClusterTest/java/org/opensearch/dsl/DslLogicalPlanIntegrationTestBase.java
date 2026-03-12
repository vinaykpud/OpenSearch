/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.dsl.queryplanner.QueryPlanExecutor;
import org.opensearch.dsl.result.QueryPlanResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Base class for DSL to Calcite integration tests.
 * Provides helper methods for plugin access and test data setup.
 */
public abstract class DslLogicalPlanIntegrationTestBase extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DslLogicalPlanPlugin.class);
    }

    /**
     * Gets a plugin instance from the test cluster.
     *
     * @param pluginClass The plugin class to retrieve
     * @param <T> The plugin type
     * @return The plugin instance
     * @throws IllegalStateException if plugin is not found
     */
    protected <T extends Plugin> T getPlugin(Class<T> pluginClass) {
        Iterable<PluginsService> services = internalCluster().getInstances(PluginsService.class);
        for (PluginsService service : services) {
            List<T> plugins = service.filterPlugins(pluginClass);
            if (!plugins.isEmpty()) {
                return plugins.get(0);
            }
        }
        throw new IllegalStateException("Plugin " + pluginClass.getName() + " not found");
    }

    /**
     * Converts a DSL query using the plugin's converter service and executor.
     * This bypasses the ActionFilter path and calls the conversion logic directly,
     * which is useful for unit-testing the conversion without a full search round-trip.
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The target index name
     * @return A SearchResponse built from the converted query plans
     * @throws Exception if conversion or execution fails
     */
    protected SearchResponse convertDsl(SearchSourceBuilder source, String indexName) throws Exception {
        DslLogicalPlanPlugin plugin = getPlugin(DslLogicalPlanPlugin.class);
        DslLogicalPlanService converterService = plugin.getConverterService();
        QueryPlanExecutor executor = plugin.getQueryPlanExecutor();

        long startTime = System.currentTimeMillis();
        QueryPlans plans = converterService.convert(source, indexName);
        QueryPlanResult result = executor.execute(plans);
        long tookInMillis = System.currentTimeMillis() - startTime;

        return SearchResponseBuilder.build(result, source, converterService.getAggregationRegistry(), tookInMillis);
    }

    /**
     * Indexes a test document with the given fields.
     *
     * @param index The index name
     * @param id The document ID
     * @param fields Field name-value pairs (name1, value1, name2, value2, ...)
     */
    protected void indexTestDoc(String index, String id, Object... fields) {
        if (fields.length % 2 != 0) {
            throw new IllegalArgumentException("Fields must be provided in name-value pairs");
        }

        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < fields.length; i += 2) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"").append(fields[i]).append("\":");
            
            Object value = fields[i + 1];
            if (value instanceof String) {
                json.append("\"").append(value).append("\"");
            } else {
                json.append(value);
            }
        }
        json.append("}");

        client().prepareIndex(index).setId(id).setSource(json.toString(), org.opensearch.common.xcontent.XContentType.JSON).get();
    }
}

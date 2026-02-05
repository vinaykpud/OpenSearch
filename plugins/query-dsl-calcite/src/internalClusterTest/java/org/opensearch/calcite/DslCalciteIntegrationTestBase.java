/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Base class for DSL to Calcite integration tests.
 * Provides helper methods for plugin access and test data setup.
 */
public abstract class DslCalciteIntegrationTestBase extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DslCalcitePlugin.class);
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

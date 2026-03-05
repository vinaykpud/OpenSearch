/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Plugin interface for providing DSL converter implementations.
 *
 * Plugins that want to provide custom DSL conversion logic should implement
 * this interface. Returns a {@link SearchResponse} or {@code null} if the
 * converter cannot handle the query (fallback to normal search).
 *
 * @opensearch.api
 */
public interface DslConverterPlugin {

    /**
     * Converts and executes an OpenSearch DSL query, returning a SearchResponse.
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A SearchResponse, or null to fall through to normal search
     * @throws Exception if conversion or execution fails
     */
    SearchResponse convertDsl(SearchSourceBuilder source, String indexName) throws Exception;
}

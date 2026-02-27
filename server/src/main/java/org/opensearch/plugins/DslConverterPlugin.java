/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Plugin interface for providing DSL converter implementations.
 * 
 * Plugins that want to provide custom DSL conversion logic should implement
 * this interface and provide the converter methods directly.
 * 
 * @opensearch.api
 */
public interface DslConverterPlugin {
    
    /**
     * Converts an OpenSearch DSL query to a plain string representation.
     * Field references use standard {@code $N} positional notation.
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A string representation of the converted query
     */
    String convertDsl(SearchSourceBuilder source, String indexName);

    /**
     * Converts an OpenSearch DSL query to an annotated string representation.
     * Field references are resolved to {@code $N:fieldName} format for human readability.
     * Intended for console and REST output.
     *
     * <p>Default implementation falls back to {@link #convertDsl}.</p>
     *
     * @param source The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return An annotated string representation of the converted query
     */
    default String convertDslAnnotated(SearchSourceBuilder source, String indexName) {
        return convertDsl(source, indexName);
    }
    
    /**
     * Gets the name of this converter implementation.
     * 
     * @return The converter name (e.g., "calcite", "datafusion")
     */
    String getConverterName();
}

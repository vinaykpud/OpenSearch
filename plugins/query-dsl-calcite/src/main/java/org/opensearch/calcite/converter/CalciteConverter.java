/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelNode;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Interface for converting OpenSearch DSL queries to Apache Calcite logical plans.
 *
 * This converter takes a SearchSourceBuilder (containing the parsed DSL query)
 * and produces a Calcite RelNode representing the logical query plan.
 */
public interface CalciteConverter {

    /**
     * Converts an OpenSearch DSL query to a Calcite RelNode.
     *
     * @param searchSource The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A Calcite RelNode representing the logical query plan
     * @throws Exception if conversion fails
     */
    RelNode convert(SearchSourceBuilder searchSource, String indexName) throws Exception;
}

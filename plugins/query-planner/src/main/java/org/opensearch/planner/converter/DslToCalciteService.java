/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.calcite.CalciteConverterService;
import org.opensearch.calcite.converter.CalciteConverter;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

/**
 * Service for converting OpenSearch DSL queries to Calcite logical plans.
 * 
 * This service wraps the CalciteConverterService from the query-dsl-calcite plugin.
 */
public class DslToCalciteService {
    
    private static final Logger logger = LogManager.getLogger(DslToCalciteService.class);
    
    private final Client client;
    private final CalciteConverterService converterService;
    
    /**
     * Constructor.
     * 
     * @param client The OpenSearch client
     * @param converterService The CalciteConverterService from query-dsl-calcite plugin
     */
    public DslToCalciteService(Client client, CalciteConverterService converterService) {
        if (converterService == null) {
            throw new IllegalArgumentException("CalciteConverterService cannot be null");
        }
        this.client = client;
        this.converterService = converterService;
        logger.info("DslToCalciteService initialized with CalciteConverterService from query-dsl-calcite plugin");
    }
    
    /**
     * Converts an OpenSearch DSL query to a Calcite logical plan.
     * 
     * @param searchSource The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A Calcite RelNode representing the logical query plan
     * @throws ConversionException if conversion fails
     */
    public RelNode convertToLogicalPlan(SearchSourceBuilder searchSource, String indexName) 
            throws ConversionException {
        try {
            CalciteConverter converter = converterService.getConverter();
            return converter.convert(searchSource, indexName);
        } catch (Exception e) {
            logger.error("Failed to convert DSL query for index: {}", indexName, e);
            throw new ConversionException(
                "Failed to convert DSL query to Calcite logical plan for index: " + indexName, 
                e
            );
        }
    }
    
    /**
     * Gets the OpenSearch client.
     * 
     * @return The Client instance
     */
    public Client getClient() {
        return client;
    }
}

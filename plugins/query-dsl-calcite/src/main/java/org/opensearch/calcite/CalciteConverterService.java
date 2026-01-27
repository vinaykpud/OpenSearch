/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.calcite.converter.CalciteConverter;
import org.opensearch.calcite.converter.CalciteConverterImpl;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;

/**
 * Service component that manages Calcite schema and provides converter instances.
 *
 * This service is responsible for:
 * - Initializing and managing the Calcite schema
 * - Providing CalciteConverter instances for DSL to RelNode conversion
 * - Registering OpenSearch indices as Calcite tables
 * - Caching index schemas for performance
 */
public class CalciteConverterService {

    private final CalciteSchema rootSchema;
    private final SchemaPlus schemaPlus;
    private final Client client;
    private final Map<String, RelDataType> schemaCache;

    /**
     * Constructor for CalciteConverterService.
     * Initializes the Calcite schema infrastructure.
     *
     * @param client The OpenSearch client for querying index mappings
     */
    public CalciteConverterService(Client client) {
        // Initialize Calcite root schema
        this.rootSchema = CalciteSchema.createRootSchema(false);
        this.schemaPlus = rootSchema.plus();
        this.client = client;
        this.schemaCache = new HashMap<>();
    }

    /**
     * Gets a CalciteConverter instance for converting DSL queries to Calcite RelNode.
     *
     * @return A new CalciteConverter instance
     */
    public CalciteConverter getConverter() {
        return new CalciteConverterImpl(schemaPlus, client, schemaCache);
    }

    /**
     * Registers an OpenSearch index as a Calcite table.
     *
     * This method allows the converter to reference the index in the logical plan.
     *
     * @param indexName The name of the OpenSearch index
     * @param rowType The RelDataType representing the index schema
     */
    public void registerTable(String indexName, RelDataType rowType) {
        // Cache the schema for this index
        schemaCache.put(indexName, rowType);
    }

    /**
     * Gets the Calcite schema.
     *
     * @return The SchemaPlus instance
     */
    public SchemaPlus getSchema() {
        return schemaPlus;
    }

    /**
     * Gets the OpenSearch client.
     *
     * @return The Client instance
     */
    public Client getClient() {
        return client;
    }

    /**
     * Gets the schema cache.
     *
     * @return The schema cache map
     */
    public Map<String, RelDataType> getSchemaCache() {
        return schemaCache;
    }
}

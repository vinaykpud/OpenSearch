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

/**
 * Service component that manages Calcite schema and provides converter instances.
 * 
 * This service is responsible for:
 * - Initializing and managing the Calcite schema
 * - Providing CalciteConverter instances for DSL to RelNode conversion
 * - Registering OpenSearch indices as Calcite tables
 */
public class CalciteConverterService {

    private final CalciteSchema rootSchema;
    private final SchemaPlus schemaPlus;

    /**
     * Constructor for CalciteConverterService.
     * Initializes the Calcite schema infrastructure.
     */
    public CalciteConverterService() {
        // Initialize Calcite root schema
        this.rootSchema = CalciteSchema.createRootSchema(false);
        this.schemaPlus = rootSchema.plus();
    }

    /**
     * Gets a CalciteConverter instance for converting DSL queries to Calcite RelNode.
     * 
     * @return A new CalciteConverter instance
     */
    public CalciteConverter getConverter() {
        return new CalciteConverterImpl(schemaPlus);
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
        // TODO: Implement table registration
        // This will be needed when we have actual index schema information
        // For POC, we'll handle schema inline in the converter
    }

    /**
     * Gets the Calcite schema.
     * 
     * @return The SchemaPlus instance
     */
    public SchemaPlus getSchema() {
        return schemaPlus;
    }
}

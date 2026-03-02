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
 * Manages the Calcite root schema and provides converter instances.
 *
 * Maintains a shared schema cache so that index mappings discovered
 * during one conversion are reused by subsequent conversions without
 * additional cluster calls.
 */
public class CalciteConverterService {

    private final CalciteSchema rootSchema;
    private final SchemaPlus schemaPlus;
    private final Client client;
    private final Map<String, RelDataType> schemaCache;

    /**
     * Creates a new CalciteConverterService.
     *
     * @param client The OpenSearch client for querying index mappings
     */
    public CalciteConverterService(Client client) {
        this.rootSchema = CalciteSchema.createRootSchema(false);
        this.schemaPlus = rootSchema.plus();
        this.client = client;
        this.schemaCache = new HashMap<>();
    }

    /** Returns a new CalciteConverter instance sharing this service's schema cache. */
    public CalciteConverter getConverter() {
        return new CalciteConverterImpl(schemaPlus, client, schemaCache);
    }

    /** Registers an index schema in the cache so future conversions can reuse it. */
    public void registerTable(String indexName, RelDataType rowType) {
        schemaCache.put(indexName, rowType);
    }

    public SchemaPlus getSchema() {
        return schemaPlus;
    }

    public Client getClient() {
        return client;
    }

    public Map<String, RelDataType> getSchemaCache() {
        return schemaCache;
    }
}

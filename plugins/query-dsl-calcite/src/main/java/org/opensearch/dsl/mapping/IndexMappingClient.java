/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.mapping;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.index.IndexNotFoundException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Client for retrieving and processing OpenSearch index mappings.
 *
 * This class provides functionality to:
 * - Query OpenSearch for index field mappings
 * - Flatten nested object structures into dot-notation paths
 * - Handle multi-field mappings (e.g., text with keyword subfield)
 *
 * Inspired by the SQL plugin's OpenSearchRestClient and IndexMapping classes.
 */
public class IndexMappingClient {

    private final Client client;
    private final Map<String, RelDataType> schemaCache = new LinkedHashMap<>();

    /**
     * Constructor for IndexMappingClient.
     *
     * @param client The OpenSearch client for querying index mappings
     */
    public IndexMappingClient(Client client) {
        this.client = client;
    }

    /**
     * Resolves the schema for an index, using the internal cache if available.
     * Fetches mappings from OpenSearch, flattens fields, maps types to Calcite, and caches the result.
     *
     * @param indexName The name of the OpenSearch index
     * @param typeFactory The Calcite type factory for building the schema
     * @return The resolved Calcite RelDataType for the index
     */
    public RelDataType resolveSchema(String indexName, RelDataTypeFactory typeFactory) {
        RelDataType cached = schemaCache.get(indexName);
        if (cached != null) {
            return cached;
        }

        Map<String, Object> properties = getMappings(indexName);
        Map<String, String> flattenedFields = flattenFields(properties);
        RelDataType schema = buildRelDataType(flattenedFields, typeFactory);

        schemaCache.put(indexName, schema);
        return schema;
    }

    private RelDataType buildRelDataType(Map<String, String> flattenedFields, RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Map.Entry<String, String> entry : flattenedFields.entrySet()) {
            builder.add(entry.getKey(), OpenSearchTypeMapper.toCalciteType(entry.getValue()));
        }
        return builder.build();
    }

    /**
     * Retrieves the field mappings for a given index.
     *
     * @param indexName The name of the OpenSearch index
     * @return A map of field names to their mapping definitions
     * @throws IndexNotFoundException if the index does not exist
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMappings(String indexName) {
        // Create request for index mappings
        GetMappingsRequest request = new GetMappingsRequest().indices(indexName);

        // Execute request synchronously
        GetMappingsResponse response = client.admin().indices().getMappings(request).actionGet();

        // Check if index exists
        if (response.mappings().isEmpty()) {
            throw new IndexNotFoundException(indexName);
        }

        // Extract mapping metadata for the index
        MappingMetadata metadata = response.mappings().get(indexName);

        // Get the properties map from the mapping source
        Map<String, Object> sourceAsMap = metadata.getSourceAsMap();
        Object properties = sourceAsMap.get("properties");

        if (properties instanceof Map) {
            return (Map<String, Object>) properties;
        }

        // Return empty map if no properties found
        return new LinkedHashMap<>();
    }

    /**
     * Flattens nested field structures into dot-notation paths.
     *
     * This method recursively traverses nested objects and multi-fields,
     * creating flattened field paths like "user.name" or "title.keyword".
     *
     * Inspired by OpenSearchDataType.traverseAndFlatten() in the SQL plugin.
     *
     * @param properties The properties map from index mappings
     * @return A flattened map where keys are field paths and values are field types
     */
    public Map<String, String> flattenFields(Map<String, Object> properties) {
        Map<String, String> result = new LinkedHashMap<>();
        flattenFieldsRecursive(properties, "", result);
        return result;
    }

    /**
     * Recursive helper method for flattening field structures.
     *
     * @param properties The current level of properties to process
     * @param prefix The current path prefix (e.g., "user" for "user.name")
     * @param result The accumulator map for flattened fields
     */
    @SuppressWarnings("unchecked")
    private void flattenFieldsRecursive(Map<String, Object> properties, String prefix, Map<String, String> result) {
        if (properties == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldDef = entry.getValue();

            // Build the full field path
            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            if (!(fieldDef instanceof Map)) {
                continue;
            }

            Map<String, Object> fieldDefMap = (Map<String, Object>) fieldDef;

            // Get the field type
            String fieldType = (String) fieldDefMap.get("type");

            // Add the field to result if it has a type
            if (fieldType != null) {
                result.put(fieldPath, fieldType);
            }

            // Handle nested objects (have "properties" key)
            Object nestedProperties = fieldDefMap.get("properties");
            if (nestedProperties instanceof Map) {
                flattenFieldsRecursive((Map<String, Object>) nestedProperties, fieldPath, result);
            }

            // Handle multi-fields (have "fields" key)
            // Example: text field with keyword subfield
            Object multiFields = fieldDefMap.get("fields");
            if (multiFields instanceof Map) {
                flattenFieldsRecursive((Map<String, Object>) multiFields, fieldPath, result);
            }
        }
    }
}

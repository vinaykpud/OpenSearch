/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;

import java.util.Map;

/**
 * Factory that creates Calcite schemas from OpenSearch cluster metadata.
 *
 * <p>This is the bridge between OpenSearch's index metadata and Calcite's
 * schema system. Each OpenSearch index becomes a Calcite table with:
 * <ul>
 *   <li>Column names and types derived from index mappings</li>
 *   <li>Row count estimates from index stats (when available)</li>
 *   <li>Metadata about which fields are indexed vs doc_values</li>
 * </ul>
 *
 * <h2>Type Mapping:</h2>
 * <pre>
 * OpenSearch Type  →  SQL Type
 * ─────────────────────────────
 * keyword, text    →  VARCHAR
 * long             →  BIGINT
 * integer          →  INTEGER
 * short            →  SMALLINT
 * byte             →  TINYINT
 * double           →  DOUBLE
 * float            →  FLOAT
 * boolean          →  BOOLEAN
 * date             →  TIMESTAMP
 * </pre>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * OpenSearchSchemaFactory factory = new OpenSearchSchemaFactory(clusterService);
 * SchemaProvider schemaProvider = factory.createSchemaProvider();
 * CalciteSqlParser parser = new CalciteSqlParser(schemaProvider, typeFactory);
 * }</pre>
 */
public class OpenSearchSchemaFactory {

    private final ClusterService clusterService;

    public OpenSearchSchemaFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Create a SchemaProvider that registers all indices as tables.
     */
    public SchemaProvider createSchemaProvider() {
        Metadata metadata = clusterService.state().metadata();
        return new OpenSearchSchemaProvider(metadata);
    }

    /**
     * SchemaProvider implementation that creates tables from OpenSearch indices.
     */
    private static class OpenSearchSchemaProvider implements SchemaProvider {
        private final Metadata metadata;

        OpenSearchSchemaProvider(Metadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public void registerTables(CalciteSchema rootSchema) {
            SchemaPlus schemaPlus = rootSchema.plus();

            for (var entry : metadata.indices().entrySet()) {
                String indexName = entry.getKey();
                IndexMetadata indexMetadata = entry.getValue();

                schemaPlus.add(indexName, new OpenSearchTable(indexName, indexMetadata));
            }
        }
    }

    /**
     * Calcite Table implementation backed by an OpenSearch index.
     *
     * <p>Provides:
     * <ul>
     *   <li>Row type (schema) from index mappings</li>
     *   <li>Statistics for cost estimation</li>
     *   <li>Index name for query execution</li>
     * </ul>
     */
    public static class OpenSearchTable extends AbstractTable {
        private final String indexName;
        private final IndexMetadata indexMetadata;

        public OpenSearchTable(String indexName, IndexMetadata indexMetadata) {
            this.indexName = indexName;
            this.indexMetadata = indexMetadata;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            var builder = typeFactory.builder();

            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> properties = (Map<String, Object>)
                    mapping.sourceAsMap().get("properties");

                if (properties != null) {
                    for (var prop : properties.entrySet()) {
                        String fieldName = prop.getKey();
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fieldProps = (Map<String, Object>) prop.getValue();
                        String fieldType = (String) fieldProps.get("type");

                        SqlTypeName sqlType = mapFieldType(fieldType);
                        // All fields are nullable by default in OpenSearch
                        builder.add(fieldName, typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(sqlType), true));
                    }
                }
            }

            // Always include _id as a system column
            builder.add("_id", typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), false));

            return builder.build();
        }

        @Override
        public Statistic getStatistic() {
            // TODO: Use actual index stats when available
            // For now, return unknown statistics
            return Statistics.UNKNOWN;
        }

        /**
         * Get the OpenSearch index name.
         */
        public String getIndexName() {
            return indexName;
        }

        /**
         * Get the index metadata.
         */
        public IndexMetadata getIndexMetadata() {
            return indexMetadata;
        }

        /**
         * Get the number of primary shards.
         */
        public int getNumberOfShards() {
            return indexMetadata.getNumberOfShards();
        }

        /**
         * Check if a field has doc_values enabled.
         */
        public boolean hasDocValues(String fieldName) {
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping == null) return false;

            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>)
                mapping.sourceAsMap().get("properties");

            if (properties == null) return false;

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldProps = (Map<String, Object>) properties.get(fieldName);
            if (fieldProps == null) return false;

            Object docValuesValue = fieldProps.get("doc_values");
            String fieldType = (String) fieldProps.get("type");

            // Check explicit setting or default based on type
            if (Boolean.TRUE.equals(docValuesValue)) {
                return true;
            }
            if (Boolean.FALSE.equals(docValuesValue)) {
                return false;
            }
            // Default: most types have doc_values enabled
            return isDocValuesEnabledByDefault(fieldType);
        }

        /**
         * Check if a field is indexed (searchable).
         */
        public boolean isFieldIndexed(String fieldName) {
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping == null) return false;

            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>)
                mapping.sourceAsMap().get("properties");

            if (properties == null) return false;

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldProps = (Map<String, Object>) properties.get(fieldName);
            if (fieldProps == null) return false;

            Object indexValue = fieldProps.get("index");
            // Default is indexed=true
            return indexValue == null || Boolean.TRUE.equals(indexValue);
        }

        private static SqlTypeName mapFieldType(String fieldType) {
            if (fieldType == null) {
                return SqlTypeName.VARCHAR;
            }
            switch (fieldType.toLowerCase()) {
                case "keyword":
                case "text":
                    return SqlTypeName.VARCHAR;
                case "long":
                    return SqlTypeName.BIGINT;
                case "integer":
                    return SqlTypeName.INTEGER;
                case "short":
                    return SqlTypeName.SMALLINT;
                case "byte":
                    return SqlTypeName.TINYINT;
                case "double":
                    return SqlTypeName.DOUBLE;
                case "float":
                    return SqlTypeName.FLOAT;
                case "boolean":
                    return SqlTypeName.BOOLEAN;
                case "date":
                    return SqlTypeName.TIMESTAMP;
                default:
                    return SqlTypeName.VARCHAR;
            }
        }

        private static boolean isDocValuesEnabledByDefault(String fieldType) {
            if (fieldType == null) return false;
            switch (fieldType.toLowerCase()) {
                case "keyword":
                case "long":
                case "integer":
                case "short":
                case "byte":
                case "double":
                case "float":
                case "date":
                case "boolean":
                case "ip":
                case "geo_point":
                    return true;
                case "text":
                    return false;  // text fields don't have doc_values by default
                default:
                    return false;
            }
        }
    }
}

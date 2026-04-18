/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.Map;

/**
 * Builds a Calcite {@link SchemaPlus} from OpenSearch {@link ClusterState} index mappings.
 *
 * <p>One Calcite table per index. Reads field types from index mapping properties.
 * Navigates: IndexMetadata -> MappingMetadata -> sourceAsMap() -> "properties" -> per-field "type".
 * // TODO: This is for illustation - use version sql plugin has built and re-purpose to not call node-client
 */
public class OpenSearchSchemaBuilder {

    private OpenSearchSchemaBuilder() {}

    /**
     * Builds a Calcite SchemaPlus from the given ClusterState.
     * Each index becomes a table; each mapped field becomes a column.
     *
     * @param clusterState the current cluster state to derive schema from
     */
    public static SchemaPlus buildSchema(ClusterState clusterState) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();

        for (Map.Entry<String, IndexMetadata> entry : clusterState.metadata().indices().entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMetadata = entry.getValue();
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> sourceMap = mapping.sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) sourceMap.get("properties");
            if (properties == null) {
                continue;
            }

            schemaPlus.add(indexName, buildTable(properties));
        }

        return schemaPlus;
    }

    /**
     * Maps an OpenSearch field type string to a Calcite SqlTypeName.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text -> VARCHAR</li>
     *   <li>long -> BIGINT</li>
     *   <li>integer -> INTEGER</li>
     *   <li>short -> SMALLINT</li>
     *   <li>byte -> TINYINT</li>
     *   <li>double -> DOUBLE</li>
     *   <li>float -> FLOAT</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>half_float -> FLOAT</li>
     *   <li>unsigned_long/scaled_float -> BIGINT</li>
     *   <li>token_count -> INTEGER</li>
     *   <li>nested/object -> skip (not mapped)</li>
     *   <li>date/date_nanos/ip/binary -> UDT (see {@link #mapFieldTypeToUdt})</li>
     *   <li>unknown -> VARCHAR (default)</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        return switch (opensearchType) {
            case "keyword", "text" -> SqlTypeName.VARCHAR;
            case "long", "unsigned_long", "scaled_float" -> SqlTypeName.BIGINT;
            case "integer", "token_count" -> SqlTypeName.INTEGER;
            case "short" -> SqlTypeName.SMALLINT;
            case "byte" -> SqlTypeName.TINYINT;
            case "double" -> SqlTypeName.DOUBLE;
            case "float", "half_float" -> SqlTypeName.FLOAT;
            case "boolean" -> SqlTypeName.BOOLEAN;
            default -> throw new IllegalArgumentException("Unsupported field type: " + opensearchType);
        };
    }

    /**
     * Returns an {@link OpenSearchFieldUDT} tag for types that need custom
     * VARCHAR-backed UDT handling, or {@code null} for types that map directly
     * to native Calcite types.
     */
    public static OpenSearchFieldUDT mapFieldTypeToUdt(String opensearchType) {
        return switch (opensearchType) {
            case "date" -> OpenSearchFieldUDT.TIMESTAMP;
            case "date_nanos" -> OpenSearchFieldUDT.TIMESTAMP_NANOS;
            case "ip" -> OpenSearchFieldUDT.IP;
            case "binary" -> OpenSearchFieldUDT.BINARY;
            default -> null;
        };
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
                    String fieldName = fieldEntry.getKey();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
                    String fieldType = (String) fieldProps.get("type");
                    if (fieldType == null) {
                        continue;
                    }
                    if ("nested".equals(fieldType) || "object".equals(fieldType)) {
                        continue;
                    }
                    OpenSearchFieldUDT udt = mapFieldTypeToUdt(fieldType);
                    if (udt != null && typeFactory instanceof OpenSearchTypeFactory osTypeFactory) {
                        String format = (String) fieldProps.get("format");
                        builder.add(fieldName, osTypeFactory.createFieldType(udt, true, format));
                    } else {
                        SqlTypeName sqlType = mapFieldType(fieldType);
                        builder.add(fieldName, typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true));
                    }
                }
                return builder.build();
            }
        };
    }
}

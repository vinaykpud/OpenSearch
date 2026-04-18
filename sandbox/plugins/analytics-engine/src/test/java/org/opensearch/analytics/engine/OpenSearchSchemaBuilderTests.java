/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.analytics.schema.OpenSearchFieldType;
import org.opensearch.analytics.schema.OpenSearchFieldUDT;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.schema.OpenSearchTypeFactory;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class OpenSearchSchemaBuilderTests extends OpenSearchTestCase {

    /**
     * Test that buildSchema produces a table for each index with correct column types.
     * Type mapping: keyword->VARCHAR, long->BIGINT, double->DOUBLE
     */
    public void testBuildSchemaWithKeywordLongDouble() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("test_index", Map.of("name", "keyword", "age", "long", "score", "double")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("test_index");
        assertNotNull("Table test_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals(3, rowType.getFieldCount());

        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
        assertFieldType(rowType, "score", SqlTypeName.DOUBLE);
    }

    /**
     * Test integer, float, boolean type mappings.
     */
    public void testBuildSchemaWithIntegerFloatBoolean() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("types_index", Map.of("count", "integer", "ratio", "float", "active", "boolean"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("types_index");
        assertNotNull("Table types_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "count", SqlTypeName.INTEGER);
        assertFieldType(rowType, "ratio", SqlTypeName.FLOAT);
        assertFieldType(rowType, "active", SqlTypeName.BOOLEAN);
    }

    /**
     * Test date, ip, text, short, byte type mappings.
     * With OpenSearchTypeFactory, date and ip fields become VARCHAR-backed UDTs.
     */
    public void testBuildSchemaWithDateIpTextShortByte() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("more_types", Map.of("created", "date", "address", "ip", "content", "text", "small_num", "short", "tiny_num", "byte"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("more_types");
        assertNotNull("Table more_types should exist in schema", table);

        RelDataType rowType = table.getRowType(OpenSearchTypeFactory.INSTANCE);
        assertUdtField(rowType, "created", OpenSearchFieldUDT.TIMESTAMP);
        assertUdtField(rowType, "address", OpenSearchFieldUDT.IP);
        assertFieldType(rowType, "content", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "small_num", SqlTypeName.SMALLINT);
        assertFieldType(rowType, "tiny_num", SqlTypeName.TINYINT);
    }

    /**
     * Test UDT types: date_nanos, binary.
     */
    public void testBuildSchemaWithDateNanosAndBinary() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("udt_types", Map.of("ts_nanos", "date_nanos", "data", "binary"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("udt_types");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(OpenSearchTypeFactory.INSTANCE);
        assertUdtField(rowType, "ts_nanos", OpenSearchFieldUDT.TIMESTAMP_NANOS);
        assertUdtField(rowType, "data", OpenSearchFieldUDT.BINARY);
    }

    /**
     * Test additional native types: half_float, unsigned_long, scaled_float, token_count.
     */
    public void testBuildSchemaWithAdditionalNativeTypes() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("native_types", Map.of("hf", "half_float", "ul", "unsigned_long", "sf", "scaled_float", "tc", "token_count"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        Table table = schema.getTable("native_types");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(OpenSearchTypeFactory.INSTANCE);
        assertFieldType(rowType, "hf", SqlTypeName.FLOAT);
        assertFieldType(rowType, "ul", SqlTypeName.BIGINT);
        assertFieldType(rowType, "sf", SqlTypeName.BIGINT);
        assertFieldType(rowType, "tc", SqlTypeName.INTEGER);
    }

    /**
     * Test that multiple indices produce multiple tables.
     */
    public void testMultipleIndicesProduceMultipleTables() throws Exception {
        IndexMetadata idx1 = buildIndexMetadata("index_a", Map.of("col1", "keyword"));
        IndexMetadata idx2 = buildIndexMetadata("index_b", Map.of("col2", "long"));

        Metadata metadata = Metadata.builder().put(idx1, false).put(idx2, false).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        assertNotNull("Table index_a should exist", schema.getTable("index_a"));
        assertNotNull("Table index_b should exist", schema.getTable("index_b"));
    }

    /**
     * Test that nested/object fields are skipped.
     */
    public void testNestedAndObjectFieldsSkipped() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("nested_index", Map.of("name", "keyword", "address", "object", "tags", "nested"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("nested_index");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Should only have 'name' field, skipping object/nested", 1, rowType.getFieldCount());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
    }

    /**
     * Test that an empty ClusterState produces an empty schema.
     */
    public void testEmptyClusterStateProducesEmptySchema() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        assertNotNull(schema);
        assertTrue("Schema should have no tables", schema.getTableNames().isEmpty());
    }

    /**
     * Test mapFieldType for all supported types.
     */
    public void testMapFieldTypeForAllSupportedTypes() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("keyword"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("text"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("long"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("unsigned_long"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("scaled_float"));
        assertEquals(SqlTypeName.INTEGER, OpenSearchSchemaBuilder.mapFieldType("integer"));
        assertEquals(SqlTypeName.INTEGER, OpenSearchSchemaBuilder.mapFieldType("token_count"));
        assertEquals(SqlTypeName.SMALLINT, OpenSearchSchemaBuilder.mapFieldType("short"));
        assertEquals(SqlTypeName.TINYINT, OpenSearchSchemaBuilder.mapFieldType("byte"));
        assertEquals(SqlTypeName.DOUBLE, OpenSearchSchemaBuilder.mapFieldType("double"));
        assertEquals(SqlTypeName.FLOAT, OpenSearchSchemaBuilder.mapFieldType("float"));
        assertEquals(SqlTypeName.FLOAT, OpenSearchSchemaBuilder.mapFieldType("half_float"));
        assertEquals(SqlTypeName.BOOLEAN, OpenSearchSchemaBuilder.mapFieldType("boolean"));
    }

    /**
     * Test mapFieldTypeToUdt returns correct UDT for supported types, null for others.
     */
    public void testMapFieldTypeToUdt() {
        assertEquals(OpenSearchFieldUDT.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldTypeToUdt("date"));
        assertEquals(OpenSearchFieldUDT.TIMESTAMP_NANOS, OpenSearchSchemaBuilder.mapFieldTypeToUdt("date_nanos"));
        assertEquals(OpenSearchFieldUDT.IP, OpenSearchSchemaBuilder.mapFieldTypeToUdt("ip"));
        assertEquals(OpenSearchFieldUDT.BINARY, OpenSearchSchemaBuilder.mapFieldTypeToUdt("binary"));
        assertNull(OpenSearchSchemaBuilder.mapFieldTypeToUdt("keyword"));
        assertNull(OpenSearchSchemaBuilder.mapFieldTypeToUdt("long"));
        assertNull(OpenSearchSchemaBuilder.mapFieldTypeToUdt("unknown"));
    }

    /**
     * Test that unknown field types throw.
     */
    public void testUnknownFieldTypeThrows() {
        expectThrows(IllegalArgumentException.class, () -> OpenSearchSchemaBuilder.mapFieldType("unknown_type"));
        expectThrows(IllegalArgumentException.class, () -> OpenSearchSchemaBuilder.mapFieldType("geo_point"));
    }

    // --- helpers ---

    private void assertFieldType(RelDataType rowType, String fieldName, SqlTypeName expectedType) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        assertNotNull("Field '" + fieldName + "' should exist", field);
        assertEquals("Field '" + fieldName + "' should have type " + expectedType, expectedType, field.getType().getSqlTypeName());
    }

    private void assertUdtField(RelDataType rowType, String fieldName, OpenSearchFieldUDT expectedUdt) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        assertNotNull("Field '" + fieldName + "' should exist", field);
        assertTrue("Field '" + fieldName + "' should be OpenSearchFieldType", field.getType() instanceof OpenSearchFieldType);
        assertEquals(expectedUdt, ((OpenSearchFieldType) field.getType()).getUdt());
        assertEquals("UDT should be VARCHAR-backed", SqlTypeName.VARCHAR, field.getType().getSqlTypeName());
    }

    private ClusterState buildClusterState(Map<String, Map<String, String>> indices) throws Exception {
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, Map<String, String>> entry : indices.entrySet()) {
            metadataBuilder.put(buildIndexMetadata(entry.getKey(), entry.getValue()), false);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(metadataBuilder.build()).build();
    }

    private IndexMetadata buildIndexMetadata(String indexName, Map<String, String> fieldTypes) throws Exception {
        StringBuilder mappingJson = new StringBuilder("{\"properties\":{");
        boolean first = true;
        for (Map.Entry<String, String> field : fieldTypes.entrySet()) {
            if (!first) mappingJson.append(",");
            mappingJson.append("\"").append(field.getKey()).append("\":{\"type\":\"").append(field.getValue()).append("\"}");
            first = false;
        }
        mappingJson.append("}}");

        return IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(mappingJson.toString())
            .build();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.calcite.mapping.IndexMappingClient;
import org.opensearch.calcite.mapping.OpenSearchTypeMapper;
import org.opensearch.transport.client.Client;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;

/**
 * Implementation of CalciteConverter that converts OpenSearch DSL to Calcite RelNode.
 *
 * This converter builds a Calcite logical plan from OpenSearch SearchSourceBuilder:
 * 1. TableScan - represents the index
 * 2. Filter - represents the query conditions
 * 3. Aggregate - represents aggregations (future)
 * 4. Sort - represents sorting and pagination (future)
 * 5. Project - represents source filtering (future)
 */
public class CalciteConverterImpl implements CalciteConverter {

    private final SchemaPlus schema;
    private final RelOptCluster cluster;
    private final RexBuilder rexBuilder;
    private final IndexMappingClient mappingClient;
    private final Map<String, RelDataType> schemaCache;

    /**
     * Constructor for CalciteConverterImpl.
     *
     * @param schema The Calcite schema
     * @param client The OpenSearch client for retrieving index mappings
     * @param schemaCache Cache for storing discovered schemas
     */
    public CalciteConverterImpl(SchemaPlus schema, Client client, Map<String, RelDataType> schemaCache) {
        this.schema = schema;
        this.mappingClient = new IndexMappingClient(client);
        this.schemaCache = schemaCache;

        // Create type factory
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // Create planner and cluster
        VolcanoPlanner planner = new VolcanoPlanner();

        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        this.rexBuilder = cluster.getRexBuilder();
    }

    /**
     * Converts an OpenSearch DSL query to a Calcite RelNode.
     *
     * @param searchSource The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return Calcite logical plan (RelNode)
     * @throws Exception if conversion fails
     */
    @Override
    public RelNode convert(SearchSourceBuilder searchSource, String indexName) throws Exception {
        // Step 1: Create table scan for the index
        RelNode tableScan = createTableScan(indexName);

        // Step 2: Apply filter if query exists
        if (searchSource.query() != null) {
            RelDataType rowType = tableScan.getRowType();
            RexNode condition = buildFilter(searchSource.query(), rowType);
            tableScan = LogicalFilter.create(tableScan, condition);
        }

        // Future steps:
        // Step 3: Apply aggregations (task 7)
        // Step 4: Apply sort and pagination (task 8)
        // Step 5: Apply projection (task 9)

        return tableScan;
    }

    /**
     * Creates a LogicalTableScan for the specified index with dynamic schema discovery.
     *
     * @param indexName The name of the index
     * @return LogicalTableScan node
     * @throws ConversionException if schema discovery fails
     */
    private RelNode createTableScan(String indexName) throws ConversionException {
        try {
            // Check if schema is already cached
            RelDataType cachedSchema = schemaCache.get(indexName);

            if (cachedSchema == null) {
                // Retrieve index mappings from OpenSearch
                Map<String, Object> properties = mappingClient.getMappings(indexName);

                // Flatten nested fields
                Map<String, String> flattenedFields = mappingClient.flattenFields(properties);

                // Build RelDataType from flattened fields
                RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();

                for (Map.Entry<String, String> entry : flattenedFields.entrySet()) {
                    String fieldName = entry.getKey();
                    String fieldType = entry.getValue();

                    // Convert OpenSearch type to Calcite type
                    SqlTypeName calciteType = OpenSearchTypeMapper.toCalciteType(fieldType);
                    builder.add(fieldName, calciteType);
                }

                cachedSchema = builder.build();

                // Cache the schema
                schemaCache.put(indexName, cachedSchema);
            }

            // Create table with discovered schema
            final RelDataType finalSchema = cachedSchema;
            Table table = new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return finalSchema;
                }
            };

            // Register table in schema
            schema.add(indexName, table);

            // Create table scan
            RelOptTable relOptTable = RelOptTableImpl.create(
                null,  // RelOptSchema
                table.getRowType(cluster.getTypeFactory()),
                List.of(indexName),
                table,
                (org.apache.calcite.linq4j.tree.Expression) null  // Expression
            );

            return LogicalTableScan.create(cluster, relOptTable, List.of());
        } catch (Exception e) {
            throw new ConversionException(null, "Failed to create table scan for index: " + indexName, e);
        }
    }

    /**
     * Builds a RexNode filter condition from an OpenSearch QueryBuilder.
     *
     * @param query The OpenSearch query
     * @param rowType The row type for field references
     * @return RexNode representing the filter condition
     */
    private RexNode buildFilter(QueryBuilder query, RelDataType rowType) {
        RexNodeQueryVisitor visitor = new RexNodeQueryVisitor(rexBuilder, rowType);

        // Dispatch to appropriate visitor method based on query type
        if (query instanceof org.opensearch.index.query.BoolQueryBuilder) {
            return visitor.visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return visitor.visitTermQuery((TermQueryBuilder) query);
        } else if (query instanceof MatchQueryBuilder) {
            return visitor.visitMatchQuery((MatchQueryBuilder) query);
        } else if (query instanceof RangeQueryBuilder) {
            return visitor.visitRangeQuery((RangeQueryBuilder) query);
        } else if (query instanceof MatchAllQueryBuilder) {
            return visitor.visitMatchAllQuery((MatchAllQueryBuilder) query);
        } else {
            throw new UnsupportedOperationException(
                "Query type not supported: " + query.getClass().getSimpleName()
            );
        }
    }
}

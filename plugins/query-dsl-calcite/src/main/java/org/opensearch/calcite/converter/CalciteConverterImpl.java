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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
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
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
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
        RelNode relNode = createTableScan(indexName);

        // Step 2: Apply filter if query exists
        if (searchSource.query() != null) {
            RelDataType rowType = relNode.getRowType();
            RexNode condition = buildFilter(searchSource.query(), rowType);
            relNode = LogicalFilter.create(relNode, condition);
        }

        // Step 3: Apply aggregations if they exist
        if (searchSource.aggregations() != null && !searchSource.aggregations().getAggregatorFactories().isEmpty()) {
            RelDataType rowType = relNode.getRowType();
            
            // Extract GROUP BY fields from bucket aggregations (terms)
            ImmutableBitSet groupBy = extractGroupByFields(searchSource, rowType);
            
            // Build metric aggregations (avg, sum, etc.)
            List<AggregateCall> aggregateCalls = buildAggregations(searchSource, rowType);
            
            // Create LogicalAggregate with GROUP BY fields and aggregate calls
            relNode = LogicalAggregate.create(
                relNode,
                groupBy,  // GROUP BY fields from terms aggregations
                null,  // no groupSets
                aggregateCalls  // metric aggregations
            );
        }

        // Future steps:
        // Step 4: Apply sort and pagination (task 8)
        // Step 5: Apply projection (task 9)

        return relNode;
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

    /**
     * Builds a list of AggregateCall objects from OpenSearch aggregations.
     * Note: Terms aggregations are handled separately in extractGroupByFields()
     * and do not produce AggregateCall objects.
     *
     * @param searchSource The SearchSourceBuilder containing aggregations
     * @param rowType The row type for field references
     * @return List of AggregateCall objects (metric aggregations only)
     * @throws ConversionException if aggregation conversion fails
     */
    private List<AggregateCall> buildAggregations(SearchSourceBuilder searchSource, RelDataType rowType) 
            throws ConversionException {
        List<AggregateCall> aggregateCalls = new ArrayList<>();
        
        // Create visitor for converting aggregations
        AggregateCallVisitor visitor = new AggregateCallVisitor(rowType);
        
        // Get aggregation factories from search source
        AggregatorFactories.Builder aggregations = searchSource.aggregations();
        
        // Iterate through all aggregations
        for (AggregationBuilder aggregation : aggregations.getAggregatorFactories()) {
            // Dispatch to appropriate visitor method based on aggregation type
            if (aggregation instanceof AvgAggregationBuilder) {
                aggregateCalls.add(visitor.visitAvgAggregation((AvgAggregationBuilder) aggregation));
            } else if (aggregation instanceof TermsAggregationBuilder) {
                // Terms aggregations are handled in extractGroupByFields() for GROUP BY
                // They do not produce AggregateCall objects
                continue;
            } else {
                throw new UnsupportedOperationException(
                    "Aggregation type not supported: " + aggregation.getClass().getSimpleName()
                );
            }
        }
        
        return aggregateCalls;
    }

    /**
     * Extracts GROUP BY fields from bucket aggregations (terms aggregations).
     * 
     * Per Requirement 20.1: "WHEN the Converter processes a terms aggregation 
     * THEN the Converter SHALL produce a LogicalAggregate with GROUP BY on the specified field"
     *
     * @param searchSource The SearchSourceBuilder containing aggregations
     * @param rowType The row type for field references
     * @return ImmutableBitSet of field indices to group by
     * @throws ConversionException if field lookup fails
     */
    private ImmutableBitSet extractGroupByFields(SearchSourceBuilder searchSource, RelDataType rowType) 
            throws ConversionException {
        List<Integer> groupByIndices = new ArrayList<>();
        
        // Get aggregation factories from search source
        AggregatorFactories.Builder aggregations = searchSource.aggregations();
        
        // Iterate through all aggregations to find bucket aggregations
        for (AggregationBuilder aggregation : aggregations.getAggregatorFactories()) {
            if (aggregation instanceof TermsAggregationBuilder) {
                TermsAggregationBuilder termsAgg = (TermsAggregationBuilder) aggregation;
                String fieldName = termsAgg.field();
                
                // Find the field index in the row type
                int fieldIndex = findFieldIndex(fieldName, rowType);
                groupByIndices.add(fieldIndex);
            }
        }
        
        // Convert list to ImmutableBitSet
        return ImmutableBitSet.of(groupByIndices);
    }

    /**
     * Finds the index of a field in the row type.
     *
     * @param fieldName The name of the field to find
     * @param rowType The row type containing field definitions
     * @return The index of the field in the row type
     * @throws ConversionException if the field is not found
     */
    private int findFieldIndex(String fieldName, RelDataType rowType) throws ConversionException {
        List<RelDataTypeField> fields = rowType.getFieldList();

        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }

        // Field not found - throw exception
        throw ConversionException.invalidField(fieldName);
    }
}

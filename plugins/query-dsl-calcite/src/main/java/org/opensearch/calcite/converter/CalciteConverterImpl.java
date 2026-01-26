/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
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
import org.apache.calcite.tools.Frameworks;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * Constructor for CalciteConverterImpl.
     * 
     * @param schema The Calcite schema
     */
    public CalciteConverterImpl(SchemaPlus schema) {
        this.schema = schema;
        
        // Create type factory
        RelDataTypeFactory typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        
        // Create planner and cluster
        org.apache.calcite.plan.volcano.VolcanoPlanner planner = 
            new org.apache.calcite.plan.volcano.VolcanoPlanner();
        
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
     * Creates a LogicalTableScan for the specified index.
     * 
     * @param indexName The name of the index
     * @return LogicalTableScan node
     */
    private RelNode createTableScan(String indexName) {
        // Create a simple table with some default fields for POC
        // In a real implementation, this would query OpenSearch for the index mapping
        Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                
                // Add some common fields for POC
                builder.add("title", SqlTypeName.VARCHAR);
                builder.add("category", SqlTypeName.VARCHAR);
                builder.add("price", SqlTypeName.DECIMAL);
                builder.add("brand", SqlTypeName.VARCHAR);
                builder.add("description", SqlTypeName.VARCHAR);
                
                return builder.build();
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
            return visitor.visitBoolQuery((org.opensearch.index.query.BoolQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.TermQueryBuilder) {
            return visitor.visitTermQuery((org.opensearch.index.query.TermQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.MatchQueryBuilder) {
            return visitor.visitMatchQuery((org.opensearch.index.query.MatchQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.RangeQueryBuilder) {
            return visitor.visitRangeQuery((org.opensearch.index.query.RangeQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.MatchAllQueryBuilder) {
            return visitor.visitMatchAllQuery((org.opensearch.index.query.MatchAllQueryBuilder) query);
        } else {
            throw new UnsupportedOperationException(
                "Query type not supported: " + query.getClass().getSimpleName()
            );
        }
    }
}

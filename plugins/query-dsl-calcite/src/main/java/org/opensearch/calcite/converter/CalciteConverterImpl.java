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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
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
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

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

        // Step 3: Apply sort if it exists (before aggregation)
        if (searchSource.sorts() != null && !searchSource.sorts().isEmpty()) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildSort(relNode, searchSource, rowType);
        }

        // Step 4: Apply aggregations if they exist
        if (searchSource.aggregations() != null && !searchSource.aggregations().getAggregatorFactories().isEmpty()) {
            // Get the input schema (before aggregation) for field lookups
            RelDataType inputRowType = relNode.getRowType();

            // Extract GROUP BY fields from bucket aggregations (terms)
            ImmutableBitSet groupBy = extractGroupByFields(searchSource, inputRowType);

            // Build metric aggregations (avg, sum, etc.)
            List<AggregateCall> aggregateCalls = buildAggregations(searchSource, inputRowType);

            // Create LogicalAggregate with GROUP BY fields and aggregate calls
            relNode = LogicalAggregate.create(
                relNode,
                groupBy,  // GROUP BY fields from terms aggregations
                null,  // no groupSets
                aggregateCalls  // metric aggregations
            );
        }

        // Step 5: Apply projection if _source filtering exists
        if (searchSource.fetchSource() != null) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildProjection(relNode, searchSource.fetchSource(), rowType);
        }

        // Step 6: Apply pagination (from/size)
        relNode = buildPagination(relNode, searchSource);

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
     * Builds a LogicalSort node from OpenSearch sort builders.
     *
     * Per Requirement 13.1: "WHEN the Converter processes a sort clause
     * THEN the Converter SHALL produce a LogicalSort with the specified field and direction"
     *
     * Per Requirement 13.2: "WHEN the Converter processes multiple sort fields
     * THEN the Converter SHALL preserve the sort order priority"
     *
     * @param input The input RelNode to apply sorting to
     * @param searchSource The SearchSourceBuilder containing sort information
     * @param rowType The row type for field references
     * @return LogicalSort node with collation
     * @throws ConversionException if sort conversion fails
     */
    private RelNode buildSort(RelNode input, SearchSourceBuilder searchSource, RelDataType rowType)
            throws ConversionException {
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        // Iterate through all sort builders
        for (SortBuilder<?> sortBuilder : searchSource.sorts()) {
            if (sortBuilder instanceof FieldSortBuilder) {
                FieldSortBuilder fieldSort = (FieldSortBuilder) sortBuilder;
                String fieldName = fieldSort.getFieldName();

                // Find the field index in the row type
                int fieldIndex = findFieldIndex(fieldName, rowType);

                // Convert OpenSearch SortOrder to Calcite Direction
                RelFieldCollation.Direction direction;
                if (fieldSort.order() == SortOrder.ASC) {
                    direction = RelFieldCollation.Direction.ASCENDING;
                } else {
                    direction = RelFieldCollation.Direction.DESCENDING;
                }

                // Handle null ordering
                // OpenSearch default: nulls last for ASC, nulls first for DESC
                RelFieldCollation.NullDirection nullDirection;
                if (fieldSort.order() == SortOrder.ASC) {
                    nullDirection = RelFieldCollation.NullDirection.LAST;
                } else {
                    nullDirection = RelFieldCollation.NullDirection.FIRST;
                }

                // Create RelFieldCollation
                RelFieldCollation fieldCollation = new RelFieldCollation(
                    fieldIndex,
                    direction,
                    nullDirection
                );

                fieldCollations.add(fieldCollation);
            } else {
                throw new UnsupportedOperationException(
                    "Sort type not supported: " + sortBuilder.getClass().getSimpleName()
                );
            }
        }

        // Create RelCollation from field collations
        RelCollation collation = RelCollations.of(fieldCollations);

        // Create LogicalSort with collation (no offset/fetch yet - that's task 9)
        return LogicalSort.create(input, collation, null, null);
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

    /**
     * Builds a LogicalProject node from OpenSearch _source filtering.
     *
     * Per Requirement 15.1: "WHEN the Converter processes a _source parameter with includes
     * THEN the Converter SHALL produce a LogicalProject with only the specified fields"
     *
     * Per Requirement 15.2: "WHEN the Converter processes _source: false
     * THEN the Converter SHALL produce a LogicalProject with no fields"
     *
     * Per Requirement 15.3: "WHEN the Converter processes _source with wildcard patterns
     * THEN the Converter SHALL expand the patterns to matching fields"
     *
     * @param input The input RelNode to apply projection to
     * @param fetchSource The FetchSourceContext containing _source filtering
     * @param rowType The row type for field references
     * @return LogicalProject node with selected fields
     * @throws ConversionException if projection conversion fails
     */
    private RelNode buildProjection(RelNode input, FetchSourceContext fetchSource, RelDataType rowType)
            throws ConversionException {
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        // Handle _source: false (no fields)
        if (!fetchSource.fetchSource()) {
            // Return empty projection (no fields)
            return LogicalProject.create(input, List.of(), projects, fieldNames);
        }

        // Get includes array
        String[] includes = fetchSource.includes();

        // If no includes specified, return all fields (skip projection)
        if (includes == null || includes.length == 0) {
            return input;
        }

        // Build list of field indices to project
        List<RelDataTypeField> allFields = rowType.getFieldList();

        for (String includePattern : includes) {
            // Check if pattern contains wildcard
            if (includePattern.contains("*")) {
                // Expand wildcard pattern
                String regex = includePattern.replace(".", "\\.").replace("*", ".*");

                for (RelDataTypeField field : allFields) {
                    if (field.getName().matches(regex)) {
                        // Add field to projection
                        projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                        fieldNames.add(field.getName());
                    }
                }
            } else {
                // Exact field name match
                int fieldIndex = findFieldIndex(includePattern, rowType);
                RelDataTypeField field = allFields.get(fieldIndex);

                // Add field to projection
                projects.add(rexBuilder.makeInputRef(field.getType(), fieldIndex));
                fieldNames.add(field.getName());
            }
        }

        // Create LogicalProject with selected fields
        return LogicalProject.create(input, List.of(), projects, fieldNames);
    }

    /**
     * Builds pagination (LIMIT/OFFSET) from OpenSearch from/size parameters.
     *
     * Per Requirement 14.1: "WHEN the Converter processes a from parameter
     * THEN the Converter SHALL produce a LogicalSort with offset"
     *
     * Per Requirement 14.2: "WHEN the Converter processes a size parameter
     * THEN the Converter SHALL produce a LogicalSort with fetch"
     *
     * Per Requirement 14.3: "WHEN the Converter processes both from and size
     * THEN the Converter SHALL produce a LogicalSort with both offset and fetch"
     *
     * Note: This method is called at the END of the pipeline, after all other operations.
     * If there's an existing LogicalSort from Step 3 (pre-aggregation sort), we should NOT
     * try to update it because:
     * 1. The schema has changed after aggregation
     * 2. The sort was on the input documents, not the aggregated results
     * 3. We need a NEW sort node on the current (post-aggregation) schema
     *
     * @param input The input RelNode to apply pagination to
     * @param searchSource The SearchSourceBuilder containing from/size parameters
     * @return LogicalSort node with offset and/or fetch, or original input if no pagination
     */
    private RelNode buildPagination(RelNode input, SearchSourceBuilder searchSource) {
        // Get from and size parameters
        // OpenSearch defaults: from=0, size=10
        int from = searchSource.from() != -1 ? searchSource.from() : 0;
        int size = searchSource.size() != -1 ? searchSource.size() : 10;

        // Only create pagination if we have non-default values
        if (from == 0 && size == 10) {
            return input;  // No pagination needed
        }

        // Create RexLiteral for offset and fetch
        RexNode offset = from > 0 ? rexBuilder.makeLiteral(from, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false) : null;
        RexNode fetch = rexBuilder.makeLiteral(size, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        // Always create a NEW LogicalSort for pagination at the end of the pipeline
        // This ensures it operates on the correct schema (post-aggregation if applicable)
        return LogicalSort.create(
            input,
            RelCollations.EMPTY,  // No sorting in pagination node, just LIMIT/OFFSET
            offset,
            fetch
        );
    }
}

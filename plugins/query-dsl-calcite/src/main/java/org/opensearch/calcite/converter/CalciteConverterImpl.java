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
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collection;
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
        RelDataType originalSchema = relNode.getRowType();

        // Step 2: Apply filter if query exists
        // Skip filter for match_all queries since LogicalTableScan already returns all rows
        if (searchSource.query() != null && !(searchSource.query() instanceof MatchAllQueryBuilder)) {
            RexNode condition = buildFilter(searchSource.query(), originalSchema);
            relNode = LogicalFilter.create(relNode, condition);
        }

        // Step 3: Apply pre-aggregation sorts (top-level sorts)
        if (searchSource.sorts() != null && !searchSource.sorts().isEmpty()) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildSort(relNode, searchSource.sorts(), rowType);
        }

        // Step 4: Apply aggregations if they exist
        AggregationInfo aggInfo = null;
        if (searchSource.aggregations() != null && !searchSource.aggregations().getAggregatorFactories().isEmpty()) {
            RelDataType inputRowType = relNode.getRowType();

            aggInfo = AggregationInfo.build(searchSource.aggregations(), inputRowType);
            ImmutableBitSet groupBy = aggInfo.getGroupByBitSet();

            List<AggregateCall> aggregateCalls = buildAggregations(searchSource, inputRowType);

            AggregateCall countCall = AggregateCall.create(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                List.of(),
                -1,
                RelCollations.EMPTY,
                cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                "_count"
            );
            aggregateCalls.add(countCall);

            relNode = LogicalAggregate.create(
                relNode,
                groupBy,
                null,
                aggregateCalls
            );
        }

        // Step 5: Apply post-aggregation sorts (from aggregation order parameters)
        if (aggInfo != null) {
            List<BucketOrder> bucketOrders = extractAggregationOrderSorts(
                searchSource.aggregations()
            );

            if (!bucketOrders.isEmpty()) {
                relNode = buildPostAggregationSort(relNode, bucketOrders, aggInfo);
            }
        }

        // Step 6: Apply projection if _source filtering exists
        if (searchSource.fetchSource() != null) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildProjection(relNode, searchSource.fetchSource(), rowType);
        }

        // Step 7: Apply pagination (from/size)
        // Skip pagination if size=0 and aggregations exist (user only wants aggregation results)
        boolean hasAggregations = searchSource.aggregations() != null &&
                                  searchSource.aggregations().getAggregatorFactories() != null &&
                                  !searchSource.aggregations().getAggregatorFactories().isEmpty();
        relNode = buildPagination(relNode, searchSource, hasAggregations);

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
     * Note: Terms aggregations are handled separately in AggregationInfo.build()
     * and do not produce AggregateCall objects, but their sub-aggregations do.
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

        // Process all aggregations (top-level and nested)
        processAggregations(searchSource.aggregations().getAggregatorFactories(), visitor, aggregateCalls);

        return aggregateCalls;
    }

    /**
     * Processes aggregations recursively, converting them to AggregateCall objects.
     * Handles both top-level and nested aggregations.
     *
     * @param aggregations Collection of aggregation builders to process
     * @param visitor Visitor for converting aggregations to AggregateCall
     * @param aggregateCalls List to add AggregateCall objects to
     * @throws ConversionException if aggregation conversion fails
     */
    private void processAggregations(
        Collection<AggregationBuilder> aggregations,
        AggregateCallVisitor visitor,
        List<AggregateCall> aggregateCalls
    ) throws ConversionException {
        if (aggregations == null || aggregations.isEmpty()) {
            return;
        }

        for (AggregationBuilder agg : aggregations) {
            if (agg instanceof AvgAggregationBuilder) {
                aggregateCalls.add(visitor.visitAvgAggregation((AvgAggregationBuilder) agg));
            } else if (agg instanceof TermsAggregationBuilder) {
                // Terms aggregations are handled in AggregationInfo.build() for GROUP BY
                // Recursively process their sub-aggregations for metric aggregations
                TermsAggregationBuilder termsAgg = (TermsAggregationBuilder) agg;
                processAggregations(termsAgg.getSubAggregations(), visitor, aggregateCalls);
            } else {
                throw new ConversionException(
                    "aggregation-conversion",
                    "Unsupported aggregation type: " + agg.getClass().getSimpleName() +
                    " (name: " + agg.getName() + "). "
                );
            }
        }
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
     * @param sorts List of SortBuilder to apply
     * @param rowType The row type for field references
     * @return LogicalSort node with collation
     * @throws ConversionException if sort conversion fails
     */
    private RelNode buildSort(RelNode input, List<SortBuilder<?>> sorts, RelDataType rowType)
            throws ConversionException {
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        // Iterate through all sort builders
        for (SortBuilder<?> sortBuilder : sorts) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort) {
                String fieldName = fieldSort.getFieldName();

                // Find the field index in the row type
                int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);

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
                int fieldIndex = SchemaUtils.findFieldIndex(includePattern, rowType);
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
     * @param hasAggregations Whether the query has aggregations
     * @return LogicalSort node with offset and/or fetch, or original input if no pagination
     */
    private RelNode buildPagination(RelNode input, SearchSourceBuilder searchSource, boolean hasAggregations) {
        // Get from and size parameters
        // OpenSearch defaults: from=0, size=10
        int from = searchSource.from() != -1 ? searchSource.from() : 0;
        int size = searchSource.size() != -1 ? searchSource.size() : 10;

        // Skip pagination if size=0 and aggregations exist
        // When size=0 with aggregations, user only wants aggregation results, not document hits
        if (size == 0 && hasAggregations) {
            return input;  // No pagination needed for aggregation-only queries
        }

        // Only create pagination if we have non-default values
        if (from == 0 && size == 10) {
            return input;  // No pagination needed
        }

        // Create RexLiteral for offset and fetch
        RexNode offset = from > 0 ? rexBuilder.makeLiteral(from, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false) : null;
        RexNode fetch = rexBuilder.makeLiteral(size, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        // Check if input is already a LogicalSort (from pre-aggregation sorting)
        // If so, merge pagination with existing sort instead of creating a new node
        if (input instanceof LogicalSort) {
            LogicalSort existingSort = (LogicalSort) input;

            // Merge pagination with existing sort collation
            return LogicalSort.create(
                existingSort.getInput(),  // Use the input of the existing sort
                existingSort.getCollation(),  // Keep the existing sort collation
                offset,
                fetch
            );
        }

        // No existing sort - create a new LogicalSort with just pagination
        return LogicalSort.create(
            input,
            RelCollations.EMPTY,  // No sorting, just LIMIT/OFFSET
            offset,
            fetch
        );
    }

    /**
     * Extracts a flat list of BucketOrder from aggregation order parameters.
     *
     * @param aggregations Aggregations from SearchSourceBuilder
     * @return List of BucketOrder elements (handles compound orders)
     */
    private List<BucketOrder> extractAggregationOrderSorts(
        AggregatorFactories.Builder aggregations
    ) {
        if (aggregations == null || aggregations.count() == 0) {
            return java.util.Collections.emptyList();
        }

        List<BucketOrder> bucketOrders = new ArrayList<>();

        for (AggregationBuilder agg : aggregations.getAggregatorFactories()) {
            if (agg instanceof TermsAggregationBuilder terms) {
                BucketOrder order = terms.order();
                if (order != null) {
                    // Handle both simple orders (single element) and compound orders (multiple elements)
                    if (order instanceof InternalOrder.CompoundOrder compound) {
                        bucketOrders.addAll(compound.orderElements());
                    } else {
                        bucketOrders.add(order);
                    }
                }
            }
        }

        return bucketOrders;
    }

    /**
     * Builds a LogicalSort node for post-aggregation sorting.
     *
     * @param input The LogicalAggregate node
     * @param bucketOrders BucketOrder list from aggregation order parameters
     * @param aggInfo Aggregation metadata and field mappings
     * @return LogicalSort node with correct field indices
     * @throws ConversionException if a sort field is not found in the post-aggregation schema
     */
    private RelNode buildPostAggregationSort(
        RelNode input,
        List<BucketOrder> bucketOrders,
        AggregationInfo aggInfo
    ) throws ConversionException {
        List<RelFieldCollation> collations = new ArrayList<>();

        for (int i = 0; i < bucketOrders.size(); i++) {
            BucketOrder bucketOrder = bucketOrders.get(i);

            try {
                // Extract all order information in one call
                OrderMetadata metadata = OrderMetadataExtractor.extract(bucketOrder);

                // Map field name to index
                int fieldIndex = aggInfo.mapSortFieldToIndex(metadata.getFieldName());

                // Create collation with consistent null handling
                RelFieldCollation collation = createCollation(fieldIndex, metadata.isAscending());

                collations.add(collation);

            } catch (ConversionException e) {
                throw new ConversionException(
                    "post-aggregation-sort",
                    "Failed to process sort order at position " + i + ": " + bucketOrder.toString(),
                    e
                );
            }
        }

        RelCollation relCollation = RelCollations.of(collations);
        return LogicalSort.create(input, relCollation, null, null);
    }

    /**
     * Creates a RelFieldCollation with consistent null direction handling.
     *
     * @param fieldIndex The index of the field to sort by
     * @param ascending True for ascending order, false for descending
     * @return RelFieldCollation with appropriate direction and null handling
     */
    private RelFieldCollation createCollation(int fieldIndex, boolean ascending) {
        RelFieldCollation.Direction direction = ascending
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;

        RelFieldCollation.NullDirection nullDirection = ascending
            ? RelFieldCollation.NullDirection.LAST
            : RelFieldCollation.NullDirection.FIRST;

        return new RelFieldCollation(fieldIndex, direction, nullDirection);
    }
}

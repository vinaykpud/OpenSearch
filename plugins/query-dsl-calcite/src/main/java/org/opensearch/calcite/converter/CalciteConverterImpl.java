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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
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
 * Converts an OpenSearch SearchSourceBuilder into a Calcite logical plan (RelNode).
 *
 * The conversion pipeline applies the following steps in order:
 * 1. TableScan — discover index schema from mappings
 * 2. Filter — convert query conditions to RexNode predicates
 * 3. Sort — convert top-level sort clauses to collations
 * 4. Aggregate — convert bucket and metric aggregations
 * 5. Post-aggregation sort — apply BucketOrder collations
 * 6. Project — apply _source field filtering
 * 7. Pagination — apply from/size as offset/fetch
 */
public class CalciteConverterImpl implements CalciteConverter {

    private final SchemaPlus schema;
    private final RelOptCluster cluster;
    private final RexBuilder rexBuilder;
    private final IndexMappingClient mappingClient;
    private final Map<String, RelDataType> schemaCache;

    /**
     * Creates a new CalciteConverterImpl.
     *
     * @param schema The Calcite schema
     * @param client The OpenSearch client for retrieving index mappings
     * @param schemaCache Cache for storing discovered index schemas
     */
    public CalciteConverterImpl(SchemaPlus schema, Client client, Map<String, RelDataType> schemaCache) {
        this.schema = schema;
        this.mappingClient = new IndexMappingClient(client);
        this.schemaCache = schemaCache;

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        VolcanoPlanner planner = new VolcanoPlanner();
        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        this.rexBuilder = cluster.getRexBuilder();
    }

    @Override
    public RelNode convert(SearchSourceBuilder searchSource, String indexName) throws Exception {
        // Step 1: Create table scan for the index
        RelNode relNode = createTableScan(indexName);
        RelDataType originalSchema = relNode.getRowType();

        // Step 2: Apply filter (skip match_all since LogicalTableScan already returns all rows)
        if (searchSource.query() != null && !(searchSource.query() instanceof MatchAllQueryBuilder)) {
            RexNode condition = buildFilter(searchSource.query(), originalSchema);
            relNode = LogicalFilter.create(relNode, condition);
        }

        // Step 3: Apply pre-aggregation sorts
        if (searchSource.sorts() != null && !searchSource.sorts().isEmpty()) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildSort(relNode, searchSource.sorts(), rowType);
        }

        // Step 4: Apply aggregations
        AggregationInfo aggInfo = null;
        if (searchSource.aggregations() != null && !searchSource.aggregations().getAggregatorFactories().isEmpty()) {
            RelDataType inputRowType = relNode.getRowType();

            aggInfo = AggregationInfo.build(searchSource.aggregations(), inputRowType);
            ImmutableBitSet groupBy = aggInfo.getGroupByBitSet();

            boolean hasGroupBy = !groupBy.isEmpty();
            List<AggregateCall> aggregateCalls = buildAggregations(searchSource, inputRowType, hasGroupBy);

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

        // Step 5: Apply post-aggregation sorts (from BucketOrder parameters)
        if (aggInfo != null && !aggInfo.getCollations().isEmpty()) {
            RelCollation relCollation = RelCollations.of(aggInfo.getCollations());
            relNode = LogicalSort.create(relNode, relCollation, null, null);
        }

        // Step 6: Apply projection if _source filtering exists
        if (searchSource.fetchSource() != null) {
            RelDataType rowType = relNode.getRowType();
            relNode = buildProjection(relNode, searchSource.fetchSource(), rowType);
        }

        // Step 7: Apply pagination (from/size)
        // Skip when size=0 with aggregations (aggregation-only queries need no document pagination)
        boolean hasAggregations = searchSource.aggregations() != null
            && searchSource.aggregations().getAggregatorFactories() != null
            && !searchSource.aggregations().getAggregatorFactories().isEmpty();
        relNode = buildPagination(relNode, searchSource, hasAggregations);

        return relNode;
    }

    /**
     * Creates a LogicalTableScan by discovering the index schema from OpenSearch mappings.
     * Fields are flattened using dot-notation for nested objects and multi-fields,
     * and each field type is mapped to the corresponding Calcite SqlTypeName.
     */
    private RelNode createTableScan(String indexName) throws ConversionException {
        try {
            RelDataType cachedSchema = schemaCache.get(indexName);

            if (cachedSchema == null) {
                Map<String, Object> properties = mappingClient.getMappings(indexName);
                Map<String, String> flattenedFields = mappingClient.flattenFields(properties);

                RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
                for (Map.Entry<String, String> entry : flattenedFields.entrySet()) {
                    SqlTypeName calciteType = OpenSearchTypeMapper.toCalciteType(entry.getValue());
                    builder.add(entry.getKey(), calciteType);
                }

                cachedSchema = builder.build();
                schemaCache.put(indexName, cachedSchema);
            }

            final RelDataType finalSchema = cachedSchema;
            Table table = new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return finalSchema;
                }
            };

            schema.add(indexName, table);

            RelOptTable relOptTable = RelOptTableImpl.create(
                null,
                table.getRowType(cluster.getTypeFactory()),
                List.of(indexName),
                table,
                (org.apache.calcite.linq4j.tree.Expression) null
            );

            return LogicalTableScan.create(cluster, relOptTable, List.of());
        } catch (Exception e) {
            throw new ConversionException(null, "Failed to create table scan for index: " + indexName, e);
        }
    }

    /**
     * Builds a RexNode filter condition from an OpenSearch QueryBuilder
     * by dispatching to the appropriate visitor method.
     */
    private RexNode buildFilter(QueryBuilder query, RelDataType rowType) {
        RexNodeQueryVisitor visitor = new RexNodeQueryVisitor(rexBuilder, rowType);

        if (query instanceof BoolQueryBuilder) {
            return visitor.visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return visitor.visitTermQuery((TermQueryBuilder) query);
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
     * Builds AggregateCall objects from metric aggregations.
     * Bucket aggregations (terms, multi_terms) are handled separately in {@link AggregationInfo}
     * as GROUP BY fields; only their sub-aggregations produce AggregateCall objects.
     */
    private List<AggregateCall> buildAggregations(SearchSourceBuilder searchSource, RelDataType rowType, boolean hasGroupBy)
            throws ConversionException {
        List<AggregateCall> aggregateCalls = new ArrayList<>();

        AggregateCallVisitor visitor = new AggregateCallVisitor(rowType, cluster.getTypeFactory(), hasGroupBy);
        processAggregations(searchSource.aggregations().getAggregatorFactories(), visitor, aggregateCalls);

        return aggregateCalls;
    }

    /**
     * Recursively processes aggregation builders, converting metric aggregations
     * to AggregateCall objects and descending into bucket aggregation sub-aggregations.
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
            } else if (agg instanceof SumAggregationBuilder) {
                aggregateCalls.add(visitor.visitSumAggregation((SumAggregationBuilder) agg));
            } else if (agg instanceof MinAggregationBuilder) {
                aggregateCalls.add(visitor.visitMinAggregation((MinAggregationBuilder) agg));
            } else if (agg instanceof MaxAggregationBuilder) {
                aggregateCalls.add(visitor.visitMaxAggregation((MaxAggregationBuilder) agg));
            } else if (agg instanceof TermsAggregationBuilder termsAgg) {
                processAggregations(termsAgg.getSubAggregations(), visitor, aggregateCalls);
            } else if (agg instanceof MultiTermsAggregationBuilder multiTermsAgg) {
                processAggregations(multiTermsAgg.getSubAggregations(), visitor, aggregateCalls);
            } else {
                throw new ConversionException(
                    "aggregation-conversion",
                    "Unsupported aggregation type: " + agg.getClass().getSimpleName()
                        + " (name: " + agg.getName() + ")"
                );
            }
        }
    }

    /**
     * Converts top-level FieldSortBuilder entries to a LogicalSort with collations.
     * Null ordering follows OpenSearch defaults: nulls last for ASC, nulls first for DESC.
     */
    private RelNode buildSort(RelNode input, List<SortBuilder<?>> sorts, RelDataType rowType)
            throws ConversionException {
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        for (SortBuilder<?> sortBuilder : sorts) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort) {
                String fieldName = fieldSort.getFieldName();
                int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);

                RelFieldCollation.Direction direction = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.Direction.ASCENDING
                    : RelFieldCollation.Direction.DESCENDING;

                RelFieldCollation.NullDirection nullDirection = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.NullDirection.LAST
                    : RelFieldCollation.NullDirection.FIRST;

                fieldCollations.add(new RelFieldCollation(fieldIndex, direction, nullDirection));
            } else {
                throw new UnsupportedOperationException(
                    "Sort type not supported: " + sortBuilder.getClass().getSimpleName()
                );
            }
        }

        RelCollation collation = RelCollations.of(fieldCollations);
        return LogicalSort.create(input, collation, null, null);
    }

    /**
     * Builds a LogicalProject from _source filtering.
     * Handles exact field names, wildcard patterns, and _source: false (empty projection).
     */
    private RelNode buildProjection(RelNode input, FetchSourceContext fetchSource, RelDataType rowType)
            throws ConversionException {
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        if (!fetchSource.fetchSource()) {
            return LogicalProject.create(input, List.of(), projects, fieldNames);
        }

        String[] includes = fetchSource.includes();
        if (includes == null || includes.length == 0) {
            return input;
        }

        List<RelDataTypeField> allFields = rowType.getFieldList();

        for (String includePattern : includes) {
            if (includePattern.contains("*")) {
                String regex = includePattern.replace(".", "\\.").replace("*", ".*");
                for (RelDataTypeField field : allFields) {
                    if (field.getName().matches(regex)) {
                        projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                        fieldNames.add(field.getName());
                    }
                }
            } else {
                int fieldIndex = SchemaUtils.findFieldIndex(includePattern, rowType);
                RelDataTypeField field = allFields.get(fieldIndex);
                projects.add(rexBuilder.makeInputRef(field.getType(), fieldIndex));
                fieldNames.add(field.getName());
            }
        }

        return LogicalProject.create(input, List.of(), projects, fieldNames);
    }

    /**
     * Applies from/size pagination as a LogicalSort with offset and fetch.
     * If the current node is already a LogicalSort (from pre-aggregation sorting),
     * the pagination is merged into that node rather than creating a new one.
     * Skipped when size=0 with aggregations (aggregation-only queries).
     */
    private RelNode buildPagination(RelNode input, SearchSourceBuilder searchSource, boolean hasAggregations) {
        int from = searchSource.from() != -1 ? searchSource.from() : 0;
        int size = searchSource.size() != -1 ? searchSource.size() : 10;

        if (size == 0 && hasAggregations) {
            return input;
        }

        if (from == 0 && size == 10) {
            return input;
        }

        RexNode offset = from > 0
            ? rexBuilder.makeLiteral(from, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false)
            : null;
        RexNode fetch = rexBuilder.makeLiteral(size, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        if (input instanceof LogicalSort existingSort) {
            return LogicalSort.create(
                existingSort.getInput(),
                existingSort.getCollation(),
                offset,
                fetch
            );
        }

        return LogicalSort.create(input, RelCollations.EMPTY, offset, fetch);
    }

}

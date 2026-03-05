/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.ConversionPipeline;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.dsl.aggregation.AggregationHandlerRegistry;
import org.opensearch.dsl.aggregation.metric.AvgMetricHandler;
import org.opensearch.dsl.aggregation.metric.MaxMetricHandler;
import org.opensearch.dsl.aggregation.metric.MinMetricHandler;
import org.opensearch.dsl.aggregation.bucket.MultiTermsBucketHandler;
import org.opensearch.dsl.aggregation.metric.CardinalityMetricHandler;
import org.opensearch.dsl.aggregation.metric.SumMetricHandler;
import org.opensearch.dsl.aggregation.bucket.TermsBucketHandler;
import org.opensearch.dsl.capabilities.AllSupportedCapabilities;
import org.opensearch.dsl.capabilities.DownstreamCapabilities;
import org.opensearch.dsl.pipeline.clause.AggregationConverter;
import org.opensearch.dsl.pipeline.clause.BucketOrderConverter;
import org.opensearch.dsl.pipeline.clause.FromSizeConverter;
import org.opensearch.dsl.pipeline.clause.IndexScanConverter;
import org.opensearch.dsl.pipeline.clause.QueryConverter;
import org.opensearch.dsl.pipeline.clause.SortConverter;
import org.opensearch.dsl.pipeline.clause.SourceConverter;
import org.opensearch.dsl.query.BoolQueryHandler;
import org.opensearch.dsl.query.MatchAllQueryHandler;
import org.opensearch.dsl.query.QueryHandlerRegistry;
import org.opensearch.dsl.query.RangeQueryHandler;
import org.opensearch.dsl.query.TermQueryHandler;
import org.opensearch.dsl.mapping.IndexMappingClient;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;



/**
 * Converts OpenSearch SearchSourceBuilder queries into Calcite logical plans (RelNode).
 *
 * Maintains a shared schema cache so that index mappings discovered during one
 * conversion are reused by subsequent conversions without additional cluster calls.
 *
 * The conversion pipeline applies the following converters in order:
 * 1. IndexMapping — discover index schema from mappings
 * 2. Query — convert query conditions to RexNode predicates
 * 3. Sort — convert top-level sort clauses to collations
 * 4. Aggregation — convert bucket and metric aggregations
 * 5. BucketOrder — apply BucketOrder collations
 * 6. Source — apply _source field filtering
 * 7. FromSize — apply from/size as offset/fetch
 */
public class DslLogicalPlanService {
    private final RelOptCluster cluster;
    private final IndexMappingClient mappingClient;
    private final DownstreamCapabilities capabilities;
    private final ConversionPipeline sharedPipeline;
    private final ConversionPipeline hitsSuffix;
    private final ConversionPipeline aggSuffix;

    /**
     * Creates a new DslLogicalPlanService with AllSupportedCapabilities (default).
     *
     * @param client The OpenSearch client for querying index mappings
     */
    public DslLogicalPlanService(Client client) {
        this(client, new AllSupportedCapabilities());
    }

    /**
     * Creates a new DslLogicalPlanService with explicit downstream capabilities.
     *
     * @param client The OpenSearch client for querying index mappings
     * @param capabilities The downstream capability checker
     */
    public DslLogicalPlanService(Client client, DownstreamCapabilities capabilities) {
        this.capabilities = capabilities;
        this.mappingClient = new IndexMappingClient(client);

//        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        VolcanoPlanner planner = new VolcanoPlanner();
        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        QueryHandlerRegistry queryRegistry = createQueryHandlerRegistry();
        AggregationHandlerRegistry aggRegistry = createAggregationHandlerRegistry();

        this.sharedPipeline = new ConversionPipeline.Builder()
            .addConverter(new IndexScanConverter())
            .addConverter(new QueryConverter(queryRegistry))
            .build();

        this.hitsSuffix = new ConversionPipeline.Builder()
            .addConverter(new SortConverter())
            .addConverter(new SourceConverter())
            .addConverter(new FromSizeConverter())
            .build();

        this.aggSuffix = new ConversionPipeline.Builder()
            .addConverter(new AggregationConverter(aggRegistry))
            .addConverter(new BucketOrderConverter())
            .build();
    }

    /**
     * Converts an OpenSearch DSL query to a QueryPlan containing one or more execution paths.
     *
     * <p>Scenarios:
     * <ul>
     *   <li>Query only (no aggs): 1 path → HITS</li>
     *   <li>Aggs with size=0: 1 path → FILTER_AGGREGATION</li>
     *   <li>Aggs with size&gt;0: 2 paths → HITS + FILTER_AGGREGATION</li>
     * </ul>
     *
     * @param searchSource The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return A QueryPlan with one or more execution paths
     * @throws Exception if conversion fails
     */
    public QueryPlan convert(SearchSourceBuilder searchSource, String indexName) throws Exception {
        RelDataType indexSchema = mappingClient.resolveSchema(
            indexName, cluster.getTypeFactory());

        ConversionContext ctx = new ConversionContext(
            searchSource,
            indexName,
            indexSchema,
            cluster,
            capabilities
        );

        RelNode shared = sharedPipeline.execute(ctx);

        boolean hasAggs = hasAggregations(searchSource);
        int size = searchSource.size() != -1 ? searchSource.size() : 10;

        QueryPlan.Builder builder = new QueryPlan.Builder();

        if (size > 0 || !hasAggs) {
            RelNode hitsTree = hitsSuffix.execute(ctx, shared);
            builder.addPath(new ExecutionPath(ExecutionPath.PathRole.HITS, hitsTree));
        }

        if (hasAggs) {
            RelNode aggTree = aggSuffix.execute(ctx, shared);
            builder.addPath(new ExecutionPath(ExecutionPath.PathRole.FILTER_AGGREGATION, aggTree));
        }

        return builder.build();
    }

    private boolean hasAggregations(SearchSourceBuilder searchSource) {
        AggregatorFactories.Builder aggs = searchSource.aggregations();
        return aggs != null
            && aggs.getAggregatorFactories() != null
            && !aggs.getAggregatorFactories().isEmpty();
    }

    private QueryHandlerRegistry createQueryHandlerRegistry() {
        QueryHandlerRegistry registry = new QueryHandlerRegistry();
        registry.register(new TermQueryHandler());
        registry.register(new RangeQueryHandler());
        registry.register(new MatchAllQueryHandler());
        registry.register(new BoolQueryHandler(registry));
        return registry;
    }

    private AggregationHandlerRegistry createAggregationHandlerRegistry() {
        AggregationHandlerRegistry registry = new AggregationHandlerRegistry();
        registry.register(new TermsBucketHandler());
        registry.register(new MultiTermsBucketHandler());
        registry.register(new AvgMetricHandler());
        registry.register(new SumMetricHandler());
        registry.register(new MinMetricHandler());
        registry.register(new MaxMetricHandler());
        registry.register(new CardinalityMetricHandler());
        return registry;
    }
}

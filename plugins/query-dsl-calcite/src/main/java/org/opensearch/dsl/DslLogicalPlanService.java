/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.aggregation.bucket.MultiTermsBucketShape;
import org.opensearch.dsl.aggregation.bucket.TermsBucketShape;
import org.opensearch.dsl.aggregation.metric.AvgMetricTranslator;
import org.opensearch.dsl.aggregation.metric.CardinalityMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MaxMetricTranslator;
import org.opensearch.dsl.aggregation.metric.MinMetricTranslator;
import org.opensearch.dsl.aggregation.metric.SumMetricTranslator;
import org.opensearch.dsl.capabilities.AllSupportedCapabilities;
import org.opensearch.dsl.capabilities.DownstreamCapabilities;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.mapping.IndexMappingClient;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.ConversionPipeline;
import org.opensearch.dsl.pipeline.converter.AggregateConverter;
import org.opensearch.dsl.pipeline.converter.PostAggregateConverter;
import org.opensearch.dsl.pipeline.converter.ScanConverter;
import org.opensearch.dsl.pipeline.converter.FilterConverter;
import org.opensearch.dsl.pipeline.converter.SortConverter;
import org.opensearch.dsl.pipeline.converter.ProjectConverter;
import org.opensearch.dsl.query.BoolQueryTranslator;
import org.opensearch.dsl.query.MatchAllQueryTranslator;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.RangeQueryTranslator;
import org.opensearch.dsl.query.TermQueryTranslator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.util.List;

/**
 * Converts OpenSearch SearchSourceBuilder queries into Calcite logical plans (RelNode).
 *
 * Maintains a shared schema cache so that index mappings discovered during one
 * conversion are reused by subsequent conversions without additional cluster calls.
 *
 * Builds separate RelNode trees for hits and aggregation paths:
 * <ul>
 *   <li><b>hitsPipeline</b>: Scan → Filter → Project → Sort</li>
 *   <li><b>aggPipeline</b>: Scan → Filter → Aggregate → PostAggregate</li>
 * </ul>
 */
public class DslLogicalPlanService {
    private final RelOptCluster cluster;
    private final IndexMappingClient mappingClient;
    private final DownstreamCapabilities capabilities;
    private final ConversionPipeline hitsPipeline;
    private final ConversionPipeline aggPipeline;
    private final AggregationRegistry aggRegistry;

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

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        QueryRegistry queryRegistry = createQueryRegistry();
        this.aggRegistry = createAggregationRegistry();

        this.hitsPipeline = new ConversionPipeline.Builder()
            .addConverter(new ScanConverter())
            .addConverter(new FilterConverter(queryRegistry))
            .addConverter(new ProjectConverter())
            .addConverter(new SortConverter())
            .build();

        this.aggPipeline = new ConversionPipeline.Builder()
            .addConverter(new ScanConverter())
            .addConverter(new FilterConverter(queryRegistry))
            .addConverter(new AggregateConverter())
            .addConverter(new PostAggregateConverter())
            .build();
    }

    /**
     * Converts an OpenSearch DSL query to QueryPlans containing one or more query plans.
     *
     * <p>Scenarios:
     * <ul>
     *   <li>Query only (no aggs): 1 plan → HITS</li>
     *   <li>Aggs with size=0: 1 plan → AGGREGATION</li>
     *   <li>Aggs with size&gt;0: 2 plans → HITS + AGGREGATION</li>
     * </ul>
     *
     * @param searchSource The SearchSourceBuilder containing the DSL query
     * @param indexName The name of the target index
     * @return QueryPlans with one or more query plans
     * @throws Exception if conversion fails
     */
    public QueryPlans convert(SearchSourceBuilder searchSource, String indexName) throws Exception {
        RelDataType indexSchema = mappingClient.resolveSchema(
            indexName, cluster.getTypeFactory());

        ConversionContext ctx = new ConversionContext(
            searchSource,
            indexName,
            indexSchema,
            cluster,
            capabilities
        );

        QueryPlans.Builder builder = new QueryPlans.Builder();

        int size = searchSource.size() != -1 ? searchSource.size() : 10;
        boolean hasAggs = hasAggregations(searchSource);

        if (size > 0 || !hasAggs) {
            RelNode hitsTree = hitsPipeline.execute(ctx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hitsTree));
        }

        if (hasAggs) {
            List<AggregationMetadata> metadataList = walkAggregations(ctx);
            for (AggregationMetadata metadata : metadataList) {
                ctx.setAggregationMetadata(metadata);
                RelNode aggTree = aggPipeline.execute(ctx);
                builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggTree));
            }
        }

        return builder.build();
    }

    private List<AggregationMetadata> walkAggregations(ConversionContext ctx) throws ConversionException {
        AggregationConversionContext aggCtx = new AggregationConversionContext(
            ctx.getIndexSchema(),
            ctx.getCluster().getTypeFactory(),
            ctx.getCapabilities()
        );
        AggregationTreeWalker walker = new AggregationTreeWalker(aggRegistry);
        return walker.walk(
            ctx.getSearchSource().aggregations().getAggregatorFactories(),
            aggCtx,
            ctx.getIndexSchema()
        );
    }

    private static boolean hasAggregations(SearchSourceBuilder searchSource) {
        AggregatorFactories.Builder aggs = searchSource.aggregations();
        return aggs != null
            && aggs.getAggregatorFactories() != null
            && !aggs.getAggregatorFactories().isEmpty();
    }

    private static QueryRegistry createQueryRegistry() {
        QueryRegistry registry = new QueryRegistry();
        registry.register(new TermQueryTranslator());
        registry.register(new RangeQueryTranslator());
        registry.register(new MatchAllQueryTranslator());
        registry.register(new BoolQueryTranslator(registry));
        return registry;
    }

    private static AggregationRegistry createAggregationRegistry() {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(new TermsBucketShape());
        registry.register(new MultiTermsBucketShape());
        registry.register(new AvgMetricTranslator());
        registry.register(new SumMetricTranslator());
        registry.register(new MinMetricTranslator());
        registry.register(new MaxMetricTranslator());
        registry.register(new CardinalityMetricTranslator());
        return registry;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.ConversionContext;
import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * Wires DSL field converters together to produce Calcite RelNode plans.
 *
 * <p>Each converter maps to one or more top-level DSL fields:
 * <ul>
 *   <li>{@link ScanConverter} — index</li>
 *   <li>{@link FilterConverter} — {@code query}</li>
 *   <li>{@link ProjectConverter} — {@code _source}</li>
 *   <li>{@link SortConverter} — {@code sort}, {@code from}, {@code size}</li>
 *   <li>{@link AggregateConverter} — {@code aggregations}</li>
 *   <li>{@link PostAggregateConverter} — aggregation bucket orders</li>
 * </ul>
 *
 * <p>The composition order is fixed relational algebra:
 * <pre>
 *     Scan
 *       └── Filter
 *             ├── Project → Sort           (HITS plan)
 *             └── Aggregate → PostAggregate (AGGREGATION plan)
 * </pre>
 */
public class SearchSourceBuilderConverter {

    private final ScanConverter scanConverter;
    private final FilterConverter filterConverter;
    private final ProjectConverter projectConverter;
    private final SortConverter sortConverter;
    private final AggregateConverter aggregateConverter;
    private final PostAggregateConverter postAggregateConverter;
    private final AggregationRegistry aggRegistry;

    /**
     * Creates a new SearchSourceBuilderConverter.
     * Converters are created once here and reused across all requests.
     *
     * @param queryRegistry the registry for resolving query translators
     * @param aggRegistry the registry for resolving aggregation types
     */
    public SearchSourceBuilderConverter(QueryRegistry queryRegistry, AggregationRegistry aggRegistry) {
        this.scanConverter = new ScanConverter();
        this.filterConverter = new FilterConverter(queryRegistry);
        this.projectConverter = new ProjectConverter();
        this.sortConverter = new SortConverter();
        this.aggregateConverter = new AggregateConverter();
        this.postAggregateConverter = new PostAggregateConverter();
        this.aggRegistry = aggRegistry;
    }

    /**
     * Converts the SearchSourceBuilder into one or more query plans.
     *
     * <p>Scenarios:
     * <ul>
     *   <li>Query only (no aggs): 1 plan → HITS</li>
     *   <li>Aggs with size=0: 1 plan → AGGREGATION</li>
     *   <li>Aggs with size&gt;0: 2 plans → HITS + AGGREGATION</li>
     * </ul>
     *
     * @param ctx the shared conversion context
     * @return query plans with one or more entries
     * @throws ConversionException if any converter fails
     */
    public QueryPlans convert(ConversionContext ctx) throws ConversionException {
        // Shared base: Scan → Filter
        RelNode base = scanConverter.convert(null, ctx);
        base = filterConverter.convert(base, ctx);

        QueryPlans.Builder builder = new QueryPlans.Builder();
        SearchSourceBuilder searchSource = ctx.getSearchSource();

        int size = searchSource.size() != -1 ? searchSource.size() : 10;
        boolean hasAggs = hasAggregations(searchSource);

        // Hits path: Filter → Project → Sort
        if (size > 0 || !hasAggs) {
            RelNode hits = projectConverter.convert(base, ctx);
            hits = sortConverter.convert(hits, ctx);
            builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.HITS, hits));
        }

        // Aggregation path: Filter → Aggregate → PostAggregate (per granularity)
        if (hasAggs) {
            List<AggregationMetadata> metadataList = walkAggregations(ctx);
            for (AggregationMetadata metadata : metadataList) {
                ctx.setAggregationMetadata(metadata);
                RelNode aggs = aggregateConverter.convert(base, ctx);
                aggs = postAggregateConverter.convert(aggs, ctx);
                builder.add(new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, aggs, metadata));
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
}

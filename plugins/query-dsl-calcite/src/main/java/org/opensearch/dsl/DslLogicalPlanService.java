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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.capabilities.AllSupportedCapabilities;
import org.opensearch.dsl.capabilities.DownstreamCapabilities;
import org.opensearch.dsl.converter.SearchSourceBuilderConverter;
import org.opensearch.dsl.mapping.IndexMappingClient;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

/**
 * Converts OpenSearch SearchSourceBuilder queries into Calcite logical plans (RelNode).
 *
 * Maintains a shared schema cache so that index mappings discovered during one
 * conversion are reused by subsequent conversions without additional cluster calls.
 *
 * Delegates to {@link SearchSourceBuilderConverter} which wires DSL field converters
 * in fixed relational algebra order:
 * <pre>
 *     Scan → Filter
 *              ├── Project → Sort           (HITS plan)
 *              └── Aggregate → PostAggregate (AGGREGATION plan)
 * </pre>
 */
public class DslLogicalPlanService {
    private final RelOptCluster cluster;
    private final IndexMappingClient mappingClient;
    private final DownstreamCapabilities capabilities;
    private final SearchSourceBuilderConverter converter;
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
        this(client, capabilities, QueryRegistryFactory.create(), AggregationRegistryFactory.create());
    }

    /**
     * Creates a new DslLogicalPlanService with explicit registries (testable injection point).
     *
     * @param client The OpenSearch client for querying index mappings
     * @param capabilities The downstream capability checker
     * @param queryRegistry The query translator registry
     * @param aggRegistry The aggregation type registry
     */
    public DslLogicalPlanService(Client client, DownstreamCapabilities capabilities,
            QueryRegistry queryRegistry, AggregationRegistry aggRegistry) {
        this.capabilities = capabilities;
        this.mappingClient = new IndexMappingClient(client);

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        this.cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        this.aggRegistry = aggRegistry;
        this.converter = new SearchSourceBuilderConverter(queryRegistry, aggRegistry);
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

        return converter.convert(ctx);
    }

    /** Returns the aggregation registry used for DSL-to-Calcite and response conversion. */
    public AggregationRegistry getAggregationRegistry() {
        return aggRegistry;
    }

}

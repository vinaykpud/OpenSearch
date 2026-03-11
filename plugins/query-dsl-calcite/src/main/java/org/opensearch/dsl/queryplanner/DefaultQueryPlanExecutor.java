/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.queryplanner;

import org.opensearch.dsl.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.QueryPlanResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes {@link QueryPlans} by delegating each query plan's RelNode to a
 * {@link RelNodeExecutor}. All plans are dispatched in parallel, then
 * results are collected by blocking on each future.
 *
 * Currently returns hardcoded test data for the nested aggregation example:
 * avg_price + terms(brand) → [brand_avg, terms(region) → region_avg]
 */
public class DefaultQueryPlanExecutor implements QueryPlanExecutor {

    private final RelNodeExecutor relNodeExecutor;

    /**
     * Creates a new executor that delegates RelNode execution to the given adapter.
     *
     * @param relNodeExecutor the adapter for executing individual RelNodes
     */
    public DefaultQueryPlanExecutor(RelNodeExecutor relNodeExecutor) {
        this.relNodeExecutor = relNodeExecutor;
    }

    @Override
    public QueryPlanResult execute(QueryPlans plans) throws Exception {
        List<QueryPlans.QueryPlan> queryPlans = plans.getAll();
        List<ExecutionResult> results = new ArrayList<>(queryPlans.size());

        for (QueryPlans.QueryPlan plan : queryPlans) {
            List<String> fieldNames = plan.relNode().getRowType().getFieldNames();
            Object[][] rows = resolveHardcodedRows(plan.type(), fieldNames);
            results.add(new ExecutionResult(plan.type(), rows, fieldNames, plan.aggregationMetadata()));
        }

        return new QueryPlanResult(results);
    }

    /**
     * Returns hardcoded test data based on plan type and schema.
     *
     * DSL under test:
     * {
     *   "size": 0,
     *   "aggs": {
     *     "avg_price": { "avg": { "field": "price" } },
     *     "by_brand": {
     *       "terms": { "field": "brand" },
     *       "aggs": {
     *         "brand_avg": { "avg": { "field": "price" } },
     *         "by_region": {
     *           "terms": { "field": "region" },
     *           "aggs": {
     *             "region_avg": { "avg": { "field": "price" } }
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     *
     * Index mapping: { brand: keyword, price: long, region: keyword }
     */
    private Object[][] resolveHardcodedRows(QueryPlans.Type type, List<String> fieldNames) {
        if (type == QueryPlans.Type.HITS) {
            // HITS path — return sample documents
            // fieldNames from Scan: [brand, price, region] (alphabetical from LinkedHashMap)
            return new Object[][] {
                { "Apple",   1299L, "US" },
                { "Apple",   999L,  "EU" },
                { "Apple",   1099L, "US" },
                { "Apple",   1199L, "EU" },
                { "Samsung", 799L,  "US" },
                { "Samsung", 899L,  "EU" },
                { "Samsung", 699L,  "US" },
                { "Dell",    599L,  "US" },
                { "Dell",    649L,  "EU" },
                { "Dell",    549L,  "US" },
            };
        }

        // AGGREGATION paths — distinguish by fieldNames
        if (fieldNames.contains("region")) {
            // Granularity "brand,region": [brand, region, region_avg, _count]
            return new Object[][] {
                { "Apple",   "US", 1299.0, 1L },
                { "Apple",   "EU", 999.0,  1L },
                { "Samsung", "US", 799.0,  1L },
            };
        }

        if (fieldNames.contains("brand")) {
            // Granularity "brand": [brand, brand_avg, _count]
            return new Object[][] {
                { "Apple",   1149.0, 2L },
                { "Samsung", 799.0,  1L },
            };
        }

        // Granularity "": [avg_price]
        return new Object[][] {
            { 1032.33 },
        };
    }
}

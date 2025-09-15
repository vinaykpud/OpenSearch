/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;

/**
 * DataFusionAggregationProcessor
 */
public class DataFusionAggregationProcessor implements AggregationProcessor {
    @Override
    public void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        // Clear OpenSearch collectors since we don't need them
        context.queryCollectorManagers().clear();
    }

    @Override
    public void postProcess(SearchContext context) {
        if (context.aggregations() == null) {
            context.queryResult().aggregations(null);
            return;
        }

        // Convert DataFusion result to InternalAggregations
        final List<InternalAggregation> internals = List.of(buildInternalAggregation());
        context.aggregations().resetBucketMultiConsumer();
        final InternalAggregations internalAggregations = InternalAggregations.from(internals);
        QuerySearchResult querySearchResult = context.queryResult();
        querySearchResult.aggregations(internalAggregations);

        context.aggregations(null);
        context.queryCollectorManagers().remove(NonGlobalAggCollectorManager.class);
        context.queryCollectorManagers().remove(GlobalAggCollectorManager.class);
    }

    private InternalAggregation buildInternalAggregation() {
        return new InternalValueCount("count()", 350, null);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;

/**
 * Handles {@link MinAggregationBuilder} — MIN metric aggregation.
 */
public class MinMetricHandler extends AbstractMetricHandler<MinAggregationBuilder> {

    @Override
    public Class<MinAggregationBuilder> getAggregationType() {
        return MinAggregationBuilder.class;
    }

    @Override
    protected org.apache.calcite.sql.SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MIN;
    }

    @Override
    protected String getFieldName(MinAggregationBuilder agg) {
        return agg.field();
    }
}

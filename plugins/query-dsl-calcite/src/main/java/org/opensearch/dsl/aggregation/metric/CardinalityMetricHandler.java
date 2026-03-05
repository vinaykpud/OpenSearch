/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;

public class CardinalityMetricHandler extends AbstractMetricHandler<CardinalityAggregationBuilder> {

    /** Creates a new Distinct Count metric handler. */
    public CardinalityMetricHandler() {}

    @Override
    public Class<CardinalityAggregationBuilder> getAggregationType() {
        return CardinalityAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
    }

    @Override
    protected String getFieldName(CardinalityAggregationBuilder agg) {
        return agg.field();
    }
}

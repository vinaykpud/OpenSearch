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
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

/**
 * Translates {@link \1} — \1 metric aggregation.
 */
public class MaxMetricTranslator extends AbstractMetricTranslator<MaxAggregationBuilder> {

    /** Creates a new MAX metric translator. */
    public MaxMetricTranslator() {}

    @Override
    public Class<MaxAggregationBuilder> getAggregationType() {
        return MaxAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.MAX;
    }

    @Override
    protected String getFieldName(MaxAggregationBuilder agg) {
        return agg.field();
    }
}

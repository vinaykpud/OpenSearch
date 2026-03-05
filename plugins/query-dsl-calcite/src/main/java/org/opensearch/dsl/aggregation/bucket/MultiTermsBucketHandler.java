/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Handles {@link MultiTermsAggregationBuilder} — multi-field bucket aggregation.
 */
public class MultiTermsBucketHandler implements BucketAggregationHandler<MultiTermsAggregationBuilder> {

    @Override
    public Class<MultiTermsAggregationBuilder> getAggregationType() {
        return MultiTermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(MultiTermsAggregationBuilder agg) {
        List<String> fields = new ArrayList<>();
        for (MultiTermsValuesSourceConfig config : agg.termsConfig()) {
            fields.add(config.getFieldName());
        }
        return new FieldGrouping(fields);
    }

    @Override
    public BucketOrder getOrder(MultiTermsAggregationBuilder agg) {
        return agg.order();
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(MultiTermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }
}

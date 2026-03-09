/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.dsl.pipeline.AbstractDslConverter;
import org.opensearch.dsl.pipeline.CollationResolver;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;

import java.util.List;

/**
 * Applies post-aggregation sorting from BucketOrder collations.
 *
 * Uses {@link CollationResolver} to resolve bucket orders against the actual
 * post-aggregation schema from the LogicalAggregate node.
 */
public class PostAggregateConverter extends AbstractDslConverter {

    /** Creates a new PostAggregateConverter for the POST_AGGREGATE phase. */
    public PostAggregateConverter() {
        super(PipelinePhase.POST_AGGREGATE);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getAggregationMetadata() != null
            && ctx.getAggregationMetadata().hasBucketOrders();
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalSort.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        List<RelFieldCollation> collations = CollationResolver.resolve(
            ctx.getAggregationMetadata(),
            input.getRowType()
        );
        if (collations.isEmpty()) {
            return input;
        }
        RelCollation relCollation = RelCollations.of(collations);
        return LogicalSort.create(input, relCollation, null, null);
    }
}

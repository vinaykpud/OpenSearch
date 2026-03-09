/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.pipeline.AbstractDslConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;

/**
 * Creates a {@code LogicalAggregate} from pre-computed {@link AggregationMetadata}
 * stored on the {@link ConversionContext}.
 *
 * The metadata is produced by {@link org.opensearch.dsl.aggregation.AggregationTreeWalker}
 * and set on the context by the service layer before running this pipeline step.
 */
public class AggregateConverter extends AbstractDslConverter {

    /** Creates a new AggregateConverter for the AGGREGATE phase. */
    public AggregateConverter() {
        super(PipelinePhase.AGGREGATE);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getAggregationMetadata() != null;
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalAggregate.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        AggregationMetadata metadata = ctx.getAggregationMetadata();
        return LogicalAggregate.create(input, metadata.getGroupByBitSet(), null, metadata.getAggregateCalls());
    }
}

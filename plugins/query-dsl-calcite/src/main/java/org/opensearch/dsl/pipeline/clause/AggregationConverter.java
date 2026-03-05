/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.clause;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.dsl.aggregation.AggregationConversionContext;
import org.opensearch.dsl.aggregation.AggregationHandlerRegistry;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.dsl.pipeline.AbstractClauseConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;

/**
 * Converts bucket and metric aggregations to a LogicalAggregate.
 * Uses the AggregationTreeWalker for a unified recursive tree walk.
 */
public class AggregationConverter extends AbstractClauseConverter {

    private final AggregationHandlerRegistry aggRegistry;

    /**
     * Creates a new AggregationConverter.
     *
     * @param aggRegistry the registry for resolving aggregation handlers
     */
    public AggregationConverter(AggregationHandlerRegistry aggRegistry) {
        super(PipelinePhase.AGGREGATE);
        this.aggRegistry = aggRegistry;
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getSearchSource().aggregations() != null
            && !ctx.getSearchSource().aggregations().getAggregatorFactories().isEmpty();
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalAggregate.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        AggregationTreeWalker walker = new AggregationTreeWalker(aggRegistry);

        AggregationConversionContext aggCtx = new AggregationConversionContext(
            input.getRowType(),
            ctx.getCluster().getTypeFactory(),
            ctx.getCapabilities()
        );

        AggregationMetadata metadata = walker.walk(
            ctx.getSearchSource().aggregations().getAggregatorFactories(),
            aggCtx,
            input.getRowType()
        );

        ctx.setAggregationMetadata(metadata);

        return LogicalAggregate.create(input, metadata.getGroupByBitSet(), null, metadata.getAggregateCalls());
    }
}

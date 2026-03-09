/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.pipeline.AbstractDslConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.index.query.MatchAllQueryBuilder;

/**
 * Converts the DSL query to a Calcite LogicalFilter.
 * Skips match_all since LogicalTableScan already returns all rows.
 */
public class FilterConverter extends AbstractDslConverter {

    private final QueryRegistry queryRegistry;

    /**
     * Creates a new FilterConverter.
     *
     * @param queryRegistry the registry for resolving query translators
     */
    public FilterConverter(QueryRegistry queryRegistry) {
        super(PipelinePhase.FILTER);
        this.queryRegistry = queryRegistry;
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getSearchSource().query() != null
            && !(ctx.getSearchSource().query() instanceof MatchAllQueryBuilder);
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalFilter.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RexNode condition = queryRegistry.convert(ctx.getSearchSource().query(), ctx);
        return LogicalFilter.create(input, condition);
    }
}

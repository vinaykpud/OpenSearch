/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.clause;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.pipeline.AbstractClauseConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Applies from/size pagination as a LogicalSort with offset and fetch.
 * Merges into existing LogicalSort if present.
 * Skipped when size=0 with aggregations (aggregation-only queries).
 */
public class FromSizeConverter extends AbstractClauseConverter {

    /** Creates a new FromSizeConverter for the LIMIT phase. */
    public FromSizeConverter() {
        super(PipelinePhase.LIMIT);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : 0;
        int size = ss.size() != -1 ? ss.size() : 10;

        // Skip when defaults (from=0, size=10)
        return !(from == 0 && size == 10);
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalSort.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : 0;
        int size = ss.size() != -1 ? ss.size() : 10;

        RexNode offset = from > 0
            ? ctx.getRexBuilder().makeLiteral(from,
                ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false)
            : null;
        RexNode fetch = ctx.getRexBuilder().makeLiteral(size,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        if (input instanceof LogicalSort existingSort) {
            return LogicalSort.create(
                existingSort.getInput(),
                existingSort.getCollation(),
                offset,
                fetch
            );
        }

        return LogicalSort.create(input, RelCollations.EMPTY, offset, fetch);
    }
}

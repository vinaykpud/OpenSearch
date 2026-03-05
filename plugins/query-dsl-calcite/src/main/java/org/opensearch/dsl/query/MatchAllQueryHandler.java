/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts a {@link MatchAllQueryBuilder} to a boolean TRUE literal.
 *
 * While top-level match_all is skipped by QueryConverter (a table scan already
 * returns all rows), match_all can appear nested inside bool queries and
 * needs a handler for that case.
 */
public class MatchAllQueryHandler implements QueryHandler {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return MatchAllQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        return ctx.getRexBuilder().makeLiteral(true);
    }
}

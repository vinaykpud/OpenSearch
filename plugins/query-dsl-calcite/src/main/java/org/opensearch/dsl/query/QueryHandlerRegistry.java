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
import org.opensearch.dsl.pipeline.HandlerRegistry;
import org.opensearch.index.query.QueryBuilder;

/**
 * Registry of {@link QueryHandler} strategies.
 *
 * Uses a map keyed by concrete QueryBuilder class for O(1) lookup.
 */
public class QueryHandlerRegistry extends HandlerRegistry<QueryBuilder, QueryHandler> {

    public QueryHandlerRegistry() {
        super("query");
    }

    public void register(QueryHandler handler) {
        doRegister(handler.getQueryType(), handler);
    }

    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        QueryHandler handler = findHandler(query);
        return handler.convert(query, ctx);
    }
}

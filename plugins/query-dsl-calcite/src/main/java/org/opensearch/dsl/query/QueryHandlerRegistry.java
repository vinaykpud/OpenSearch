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

    /** Creates a new query handler registry. */
    public QueryHandlerRegistry() {
        super("query");
    }

    /**
     * Registers a query handler.
     *
     * @param handler the handler to register
     */
    public void register(QueryHandler handler) {
        doRegister(handler.getQueryType(), handler);
    }

    /**
     * Converts a query to a Calcite RexNode using the registered handler.
     *
     * @param query the query builder to convert
     * @param ctx the conversion context
     * @return the resulting RexNode filter expression
     * @throws ConversionException if no handler is found or conversion fails
     */
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        QueryHandler handler = findHandler(query);
        return handler.convert(query, ctx);
    }
}

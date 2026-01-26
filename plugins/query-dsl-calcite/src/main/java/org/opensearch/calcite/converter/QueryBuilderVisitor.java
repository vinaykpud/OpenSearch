/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

/**
 * Visitor interface for converting OpenSearch QueryBuilder types to a target type T.
 * 
 * This interface follows the Visitor pattern to handle different query types
 * and convert them to the appropriate representation (e.g., Calcite RexNode).
 *
 * @param <T> The target type to convert queries into (e.g., RexNode)
 */
public interface QueryBuilderVisitor<T> {
    
    /**
     * Visits a BoolQueryBuilder and converts it to the target type.
     * 
     * @param query The bool query containing must, filter, should, must_not clauses
     * @return The converted representation
     */
    T visitBoolQuery(BoolQueryBuilder query);
    
    /**
     * Visits a TermQueryBuilder and converts it to the target type.
     * 
     * @param query The term query for exact matching
     * @return The converted representation
     */
    T visitTermQuery(TermQueryBuilder query);
    
    /**
     * Visits a MatchQueryBuilder and converts it to the target type.
     * 
     * @param query The match query for full-text search
     * @return The converted representation
     */
    T visitMatchQuery(MatchQueryBuilder query);
    
    /**
     * Visits a RangeQueryBuilder and converts it to the target type.
     * 
     * @param query The range query with gte, lte, gt, lt conditions
     * @return The converted representation
     */
    T visitRangeQuery(RangeQueryBuilder query);
    
    /**
     * Visits a MatchAllQueryBuilder and converts it to the target type.
     * 
     * @param query The match all query
     * @return The converted representation
     */
    T visitMatchAllQuery(MatchAllQueryBuilder query);
}

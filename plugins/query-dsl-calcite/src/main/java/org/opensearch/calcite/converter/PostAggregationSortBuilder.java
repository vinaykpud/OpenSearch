/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.opensearch.calcite.exception.ConversionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds LogicalSort nodes for post-aggregation sorting.
 * 
 * This class is responsible for:
 * 1. Converting aggregation order sorts to RelFieldCollation
 * 2. Mapping sort field names to post-aggregation schema indices
 * 3. Creating LogicalSort with correct collation
 * 4. Handling multiple sort criteria
 */
public class PostAggregationSortBuilder {
    private final RexBuilder rexBuilder;
    private final AggregationInfo aggInfo;
    
    /**
     * Constructor.
     * 
     * @param rexBuilder RexBuilder for creating expressions
     * @param aggInfo Aggregation metadata and field mappings
     */
    public PostAggregationSortBuilder(RexBuilder rexBuilder, AggregationInfo aggInfo) {
        this.rexBuilder = rexBuilder;
        this.aggInfo = aggInfo;
    }
    
    /**
     * Builds a LogicalSort node for post-aggregation sorting.
     * 
     * @param input The LogicalAggregate node
     * @param orderSorts Sort specifications from aggregation order parameters
     * @return LogicalSort node with correct field indices
     * @throws ConversionException if a sort field is not found in the post-aggregation schema
     */
    public LogicalSort buildPostAggregationSort(
        RelNode input,
        List<SortAnalyzer.AggregationOrderSort> orderSorts
    ) throws ConversionException {
        List<RelFieldCollation> collations = new ArrayList<>();
        
        for (SortAnalyzer.AggregationOrderSort orderSort : orderSorts) {
            String sortField = orderSort.getField();
            boolean ascending = orderSort.isAscending();
            
            // Map sort field to index in post-aggregation schema
            // This may throw ConversionException if field not found
            int fieldIndex = aggInfo.mapSortFieldToIndex(sortField);
            
            // Create RelFieldCollation
            RelFieldCollation.Direction direction = ascending
                ? RelFieldCollation.Direction.ASCENDING
                : RelFieldCollation.Direction.DESCENDING;
            
            // Default null handling: nulls last for ascending, nulls first for descending
            RelFieldCollation.NullDirection nullDirection = ascending
                ? RelFieldCollation.NullDirection.LAST
                : RelFieldCollation.NullDirection.FIRST;
            
            RelFieldCollation collation = new RelFieldCollation(
                fieldIndex,
                direction,
                nullDirection
            );
            
            collations.add(collation);
        }
        
        RelCollation relCollation = RelCollations.of(collations);
        
        return LogicalSort.create(
            input,
            relCollation,
            null,  // offset
            null   // fetch
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.util.Collections;

/**
 * Converts OpenSearch metric aggregation builders to Calcite AggregateCall objects.
 *
 * Result type nullability depends on whether a GROUP BY is present:
 * with GROUP BY, aggregates are non-nullable (at least one group exists);
 * without GROUP BY (global aggregation), they are nullable (no input rows → null).
 */
public class AggregateCallVisitor implements AggregationBuilderVisitor {

    private final RelDataType rowType;
    private final RelDataTypeFactory typeFactory;
    private final boolean hasGroupBy;

    public AggregateCallVisitor(RelDataType rowType, RelDataTypeFactory typeFactory, boolean hasGroupBy) {
        this.rowType = rowType;
        this.typeFactory = typeFactory;
        this.hasGroupBy = hasGroupBy;
    }

    @Override
    public AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException {
        return buildMetricCall(SqlStdOperatorTable.AVG, aggregation.field(), aggregation.getName());
    }

    @Override
    public AggregateCall visitSumAggregation(SumAggregationBuilder aggregation) throws ConversionException {
        return buildMetricCall(SqlStdOperatorTable.SUM, aggregation.field(), aggregation.getName());
    }

    @Override
    public AggregateCall visitMinAggregation(MinAggregationBuilder aggregation) throws ConversionException {
        return buildMetricCall(SqlStdOperatorTable.MIN, aggregation.field(), aggregation.getName());
    }

    @Override
    public AggregateCall visitMaxAggregation(MaxAggregationBuilder aggregation) throws ConversionException {
        return buildMetricCall(SqlStdOperatorTable.MAX, aggregation.field(), aggregation.getName());
    }

    private AggregateCall buildMetricCall(SqlAggFunction function, String fieldName, String aggName)
            throws ConversionException {
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        RelDataType fieldType = rowType.getFieldList().get(fieldIndex).getType();
        RelDataType resultType = hasGroupBy
            ? fieldType
            : typeFactory.createTypeWithNullability(fieldType, true);

        return AggregateCall.create(
            function,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            resultType,
            aggName
        );
    }
}

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.calcite.exception.ConversionException;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.util.Collections;

/**
 * Implementation of AggregationBuilderVisitor that converts OpenSearch metric aggregations
 * to Calcite AggregateCall objects.
 */
public class AggregateCallVisitor implements AggregationBuilderVisitor {

    private final RelDataType rowType;

    /**
     * Creates a new AggregateCallVisitor.
     *
     * @param rowType the row type of the input relation
     */
    public AggregateCallVisitor(RelDataType rowType) {
        this.rowType = rowType;
    }

    @Override
    public AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction avgFunction = SqlStdOperatorTable.AVG;

        return AggregateCall.create(
            avgFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            rowType.getFieldList().get(fieldIndex).getType(),
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitSumAggregation(SumAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction sumFunction = SqlStdOperatorTable.SUM;

        return AggregateCall.create(
            sumFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            rowType.getFieldList().get(fieldIndex).getType(),
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitMinAggregation(MinAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction minFunction = SqlStdOperatorTable.MIN;

        return AggregateCall.create(
            minFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            rowType.getFieldList().get(fieldIndex).getType(),
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitMaxAggregation(MaxAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction maxFunction = SqlStdOperatorTable.MAX;

        return AggregateCall.create(
            maxFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            rowType.getFieldList().get(fieldIndex).getType(),
            aggregation.getName()
        );
    }
}

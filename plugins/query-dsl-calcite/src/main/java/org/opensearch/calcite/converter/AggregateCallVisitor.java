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
 * Implementation of AggregationBuilderVisitor that converts OpenSearch metric aggregations
 * to Calcite AggregateCall objects.
 */
public class AggregateCallVisitor implements AggregationBuilderVisitor {

    private final RelDataType rowType;
    private final RelDataTypeFactory typeFactory;
    private final boolean hasGroupBy;

    /**
     * Creates a new AggregateCallVisitor.
     *
     * @param rowType the row type of the input relation
     * @param typeFactory the type factory for creating nullable types
     * @param hasGroupBy whether the query has a GROUP BY clause (non-empty group set)
     */
    public AggregateCallVisitor(RelDataType rowType, RelDataTypeFactory typeFactory, boolean hasGroupBy) {
        this.rowType = rowType;
        this.typeFactory = typeFactory;
        this.hasGroupBy = hasGroupBy;
    }

    @Override
    public AggregateCall visitAvgAggregation(AvgAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction avgFunction = SqlStdOperatorTable.AVG;

        // Get the field type
        RelDataType fieldType = rowType.getFieldList().get(fieldIndex).getType();
        
        // Make type nullable only when there's NO GROUP BY
        // With GROUP BY: aggregates return non-nullable types (always at least one group)
        // Without GROUP BY: aggregates return nullable types (can be null when no input rows)
        RelDataType resultType = hasGroupBy 
            ? fieldType 
            : typeFactory.createTypeWithNullability(fieldType, true);

        return AggregateCall.create(
            avgFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            resultType,
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitSumAggregation(SumAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction sumFunction = SqlStdOperatorTable.SUM;

        // Get the field type
        RelDataType fieldType = rowType.getFieldList().get(fieldIndex).getType();
        
        // Make type nullable only when there's NO GROUP BY
        // With GROUP BY: aggregates return non-nullable types (always at least one group)
        // Without GROUP BY: aggregates return nullable types (can be null when no input rows)
        RelDataType resultType = hasGroupBy 
            ? fieldType 
            : typeFactory.createTypeWithNullability(fieldType, true);

        return AggregateCall.create(
            sumFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            resultType,
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitMinAggregation(MinAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction minFunction = SqlStdOperatorTable.MIN;

        // Get the field type
        RelDataType fieldType = rowType.getFieldList().get(fieldIndex).getType();
        
        // Make type nullable only when there's NO GROUP BY
        // With GROUP BY: aggregates return non-nullable types (always at least one group)
        // Without GROUP BY: aggregates return nullable types (can be null when no input rows)
        RelDataType resultType = hasGroupBy 
            ? fieldType 
            : typeFactory.createTypeWithNullability(fieldType, true);

        return AggregateCall.create(
            minFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            resultType,
            aggregation.getName()
        );
    }

    @Override
    public AggregateCall visitMaxAggregation(MaxAggregationBuilder aggregation) throws ConversionException {
        String fieldName = aggregation.field();
        int fieldIndex = SchemaUtils.findFieldIndex(fieldName, rowType);
        SqlAggFunction maxFunction = SqlStdOperatorTable.MAX;

        // Get the field type
        RelDataType fieldType = rowType.getFieldList().get(fieldIndex).getType();
        
        // Make type nullable only when there's NO GROUP BY
        // With GROUP BY: aggregates return non-nullable types (always at least one group)
        // Without GROUP BY: aggregates return nullable types (can be null when no input rows)
        RelDataType resultType = hasGroupBy 
            ? fieldType 
            : typeFactory.createTypeWithNullability(fieldType, true);

        return AggregateCall.create(
            maxFunction,
            false,
            false,
            false,
            Collections.singletonList(fieldIndex),
            -1,
            RelCollations.EMPTY,
            resultType,
            aggregation.getName()
        );
    }
}

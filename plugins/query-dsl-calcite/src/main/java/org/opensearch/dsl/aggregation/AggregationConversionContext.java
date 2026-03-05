/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.dsl.capabilities.DownstreamCapabilities;
import org.opensearch.dsl.exception.ConversionException;

/**
 * Context for aggregation-to-AggregateCall conversion.
 *
 * Wraps the row type, type factory, and downstream capabilities needed
 * by individual aggregation handler implementations.
 */
public class AggregationConversionContext {

    private final RelDataType rowType;
    private final RelDataTypeFactory typeFactory;
    private final DownstreamCapabilities capabilities;

    public AggregationConversionContext(
        RelDataType rowType,
        RelDataTypeFactory typeFactory,
        DownstreamCapabilities capabilities
    ) {
        this.rowType = rowType;
        this.typeFactory = typeFactory;
        this.capabilities = capabilities;
    }

    public RelDataType getRowType() {
        return rowType;
    }

    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public void requireAggFunctionSupported(SqlAggFunction function) throws ConversionException {
        if (!capabilities.isAggFunctionSupported(function)) {
            throw ConversionException.unsupportedAggFunction(function.getName());
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.capabilities.DownstreamCapabilities;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Shared state mediator for the conversion pipeline.
 *
 * Carries immutable inputs (set at construction) and mutable shared state
 * (written by converters, read by downstream converters). Also provides convenience
 * methods for downstream capability validation.
 */
public class ConversionContext {

    // Immutable inputs
    private final SearchSourceBuilder searchSource;
    private final String indexName;
    private final RelDataType indexSchema;
    private final RelOptCluster cluster;
    private final DownstreamCapabilities capabilities;

    // Mutable shared state
    private AggregationMetadata aggregationMetadata;

    public ConversionContext(
        SearchSourceBuilder searchSource,
        String indexName,
        RelDataType indexSchema,
        RelOptCluster cluster,
        DownstreamCapabilities capabilities
    ) {
        this.searchSource = searchSource;
        this.indexName = indexName;
        this.indexSchema = indexSchema;
        this.cluster = cluster;
        this.capabilities = capabilities;
    }

    // --- Immutable accessors ---

    public SearchSourceBuilder getSearchSource() {
        return searchSource;
    }

    public String getIndexName() {
        return indexName;
    }

    public RelDataType getIndexSchema() {
        return indexSchema;
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    public RexBuilder getRexBuilder() {
        return cluster.getRexBuilder();
    }

    public DownstreamCapabilities getCapabilities() {
        return capabilities;
    }

    // --- Mutable shared state ---

    public AggregationMetadata getAggregationMetadata() {
        return aggregationMetadata;
    }

    public void setAggregationMetadata(AggregationMetadata aggregationMetadata) {
        this.aggregationMetadata = aggregationMetadata;
    }

    public RelDataType getRowType() {
        return indexSchema;
    }

    public void requireOperatorSupported(SqlOperator operator) throws ConversionException {
        if (!capabilities.isOperatorSupported(operator)) {
            throw ConversionException.unsupportedOperator(operator.getName());
        }
    }

    // --- Validation convenience methods ---

    public void requireRelNodeSupported(Class<? extends RelNode> type) throws ConversionException {
        if (!capabilities.isRelNodeSupported(type)) {
            throw ConversionException.unsupportedRelNode(type.getSimpleName());
        }
    }
}

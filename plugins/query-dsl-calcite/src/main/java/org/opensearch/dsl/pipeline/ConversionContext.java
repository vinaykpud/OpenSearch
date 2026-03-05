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

    /**
     * Creates a new conversion context.
     *
     * @param searchSource the original search request
     * @param indexName    the target index name
     * @param indexSchema  the resolved index schema
     * @param cluster      the Calcite cluster for plan construction
     * @param capabilities downstream execution capabilities
     */
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

    /** Returns the original search source builder. */
    public SearchSourceBuilder getSearchSource() {
        return searchSource;
    }

    /** Returns the target index name. */
    public String getIndexName() {
        return indexName;
    }

    /** Returns the resolved index schema. */
    public RelDataType getIndexSchema() {
        return indexSchema;
    }

    /** Returns the Calcite cluster used for plan construction. */
    public RelOptCluster getCluster() {
        return cluster;
    }

    /** Returns the {@link RexBuilder} from the cluster. */
    public RexBuilder getRexBuilder() {
        return cluster.getRexBuilder();
    }

    /** Returns the downstream execution capabilities. */
    public DownstreamCapabilities getCapabilities() {
        return capabilities;
    }

    // --- Mutable shared state ---

    /** Returns the aggregation metadata, or {@code null} if not yet set. */
    public AggregationMetadata getAggregationMetadata() {
        return aggregationMetadata;
    }

    /**
     * Sets the aggregation metadata produced by the aggregation converter.
     *
     * @param aggregationMetadata the metadata to store
     */
    public void setAggregationMetadata(AggregationMetadata aggregationMetadata) {
        this.aggregationMetadata = aggregationMetadata;
    }

    /** Returns the row type of the index schema. */
    public RelDataType getRowType() {
        return indexSchema;
    }

    /**
     * Throws if the given SQL operator is not supported downstream.
     *
     * @param operator the operator to check
     * @throws ConversionException if the operator is unsupported
     */
    public void requireOperatorSupported(SqlOperator operator) throws ConversionException {
        if (!capabilities.isOperatorSupported(operator)) {
            throw ConversionException.unsupportedOperator(operator.getName());
        }
    }

    // --- Validation convenience methods ---

    /**
     * Throws if the given RelNode type is not supported downstream.
     *
     * @param type the RelNode class to check
     * @throws ConversionException if the RelNode type is unsupported
     */
    public void requireRelNodeSupported(Class<? extends RelNode> type) throws ConversionException {
        if (!capabilities.isRelNodeSupported(type)) {
            throw ConversionException.unsupportedRelNode(type.getSimpleName());
        }
    }
}

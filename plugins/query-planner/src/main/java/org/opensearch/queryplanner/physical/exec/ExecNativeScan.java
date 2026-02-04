/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.exec;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Native scan node - reads from columnar data sources directly.
 *
 * <p>Unlike {@link ExecScan} which reads from Lucene via OpenSearch,
 * this scan is executed directly by the native engine (e.g., DataFusion).
 * The engine receives the scan specification and performs the read itself.
 *
 * <p>Supports:
 * <ul>
 *   <li>Column projection - only read specified columns</li>
 *   <li>Filter pushdown - predicate applied at scan time (e.g., Parquet row group filtering)</li>
 * </ul>
 *
 * <h2>Use Cases:</h2>
 * <ul>
 *   <li>Pure columnar scan: Reading Parquet files directly</li>
 *   <li>Hybrid scan: Columnar scan with filter applied at scan time</li>
 * </ul>
 */
public class ExecNativeScan implements ExecNode {

    /** Distribution type: data originates from shards in an unpartitioned manner. */
    public static final String DISTRIBUTION_SOURCE_RANDOM = "SOURCE_RANDOM";

    private final String tableName;
    private final List<String> columns;
    private final String filter;  // null means no filter
    private final String distribution;

    /**
     * Create a native scan without filter.
     */
    public ExecNativeScan(String tableName, List<String> columns) {
        this(tableName, columns, null, DISTRIBUTION_SOURCE_RANDOM);
    }

    /**
     * Create a native scan with optional filter.
     */
    public ExecNativeScan(String tableName, List<String> columns, String filter) {
        this(tableName, columns, filter, DISTRIBUTION_SOURCE_RANDOM);
    }

    /**
     * Create a native scan with all parameters.
     */
    public ExecNativeScan(String tableName, List<String> columns, String filter, String distribution) {
        this.tableName = tableName;
        this.columns = columns;
        this.filter = filter;
        this.distribution = distribution;
    }

    /**
     * Deserialize from stream.
     */
    public ExecNativeScan(StreamInput in) throws IOException {
        this.tableName = in.readString();
        this.columns = in.readStringList();
        this.filter = in.readOptionalString();
        this.distribution = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tableName);
        out.writeStringCollection(columns);
        out.writeOptionalString(filter);
        out.writeOptionalString(distribution);
    }

    @Override
    public NodeType getType() {
        return NodeType.NATIVE_SCAN;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.emptyList();
    }

    /**
     * Get the table/index name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the columns to read.
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * Get the filter expression (may be null).
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Check if this scan has a filter.
     */
    public boolean hasFilter() {
        return filter != null && !filter.isEmpty();
    }

    /**
     * Get the distribution type.
     */
    public String getDistribution() {
        return distribution != null ? distribution : DISTRIBUTION_SOURCE_RANDOM;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExecNativeScan[");
        sb.append(tableName);
        sb.append(", columns=").append(columns);
        if (filter != null) {
            sb.append(", filter=").append(filter);
        }
        sb.append(", dist=").append(getDistribution());
        sb.append("]");
        return sb.toString();
    }
}

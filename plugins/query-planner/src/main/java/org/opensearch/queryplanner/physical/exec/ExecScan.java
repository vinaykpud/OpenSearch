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
 * Scan node - reads from an index.
 *
 * <p>Supports:
 * <ul>
 *   <li>Column projection - only read specified columns</li>
 *   <li>Filter pushdown - Lucene query for filtering at scan time</li>
 * </ul>
 */
public class ExecScan implements ExecNode {

    /** Distribution type: data originates from shards in an unpartitioned manner. */
    public static final String DISTRIBUTION_SOURCE_RANDOM = "SOURCE_RANDOM";

    private final String indexName;
    private final List<String> columns;
    private final String filter;  // null means no filter
    private final String distribution;

    public ExecScan(String indexName, List<String> columns) {
        this(indexName, columns, null, DISTRIBUTION_SOURCE_RANDOM);
    }

    public ExecScan(String indexName, List<String> columns, String filter) {
        this(indexName, columns, filter, DISTRIBUTION_SOURCE_RANDOM);
    }

    public ExecScan(String indexName, List<String> columns, String filter, String distribution) {
        this.indexName = indexName;
        this.columns = columns;
        this.filter = filter;
        this.distribution = distribution;
    }

    public ExecScan(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.columns = in.readStringList();
        this.filter = in.readOptionalString();
        // Read distribution with default for backward compatibility
        this.distribution = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeStringCollection(columns);
        out.writeOptionalString(filter);
        out.writeOptionalString(distribution);
    }

    @Override
    public NodeType getType() {
        return NodeType.SCAN;
    }

    @Override
    public List<ExecNode> getChildren() {
        return Collections.emptyList();
    }

    public String getIndexName() {
        return indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getFilter() {
        return filter;
    }

    public boolean hasFilter() {
        return filter != null && !filter.isEmpty();
    }

    public String getDistribution() {
        return distribution != null ? distribution : DISTRIBUTION_SOURCE_RANDOM;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExecScan[");
        sb.append(indexName);
        sb.append(", columns=").append(columns);
        if (filter != null) {
            sb.append(", filter=").append(filter);
        }
        sb.append(", dist=").append(getDistribution());
        sb.append("]");
        return sb.toString();
    }
}

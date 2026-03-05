/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.rel.RelNode;

/**
 * A single execution path within a {@link QueryPlan}, pairing a Calcite RelNode
 * with its role in constructing the final SearchResponse.
 */
public final class ExecutionPath {

    /**
     * Identifies what part of the SearchResponse this execution path populates.
     */
    public enum PathRole {
        /** Document hits (query results with pagination). */
        HITS,
        /** Aggregation results computed over filtered data. */
        FILTER_AGGREGATION,
        /** Suggestion results. */
        SUGGESTION,
        /** Query profiling data. */
        PROFILE
    }

    private final PathRole role;
    private final RelNode relNode;

    /**
     * Creates an execution path with the given role and relational node.
     *
     * @param role    the role identifying what part of the response this path populates
     * @param relNode the Calcite relational node representing this path's plan
     */
    public ExecutionPath(PathRole role, RelNode relNode) {
        this.role = role;
        this.relNode = relNode;
    }

    /**
     * Returns the role of this execution path.
     *
     * @return the path role
     */
    public PathRole getRole() {
        return role;
    }

    /**
     * Returns the Calcite relational node for this execution path.
     *
     * @return the RelNode
     */
    public RelNode getRelNode() {
        return relNode;
    }
}

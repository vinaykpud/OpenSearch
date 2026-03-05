/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Container for one or more {@link ExecutionPath}s produced by DSL-to-RelNode conversion.
 *
 * A single DSL query may require multiple independent RelNode trees when the results
 * cannot be represented in one tree (e.g., aggregation with size &gt; 0 needs both
 * a HITS path and a FILTER_AGGREGATION path).
 */
public final class QueryPlan {

    private final List<ExecutionPath> paths;

    private QueryPlan(List<ExecutionPath> paths) {
        this.paths = List.copyOf(paths);
    }

    /**
     * Returns all execution paths in this query plan.
     *
     * @return an unmodifiable list of execution paths
     */
    public List<ExecutionPath> getAllPaths() {
        return paths;
    }

    /**
     * Returns the first execution path matching the given role, if any.
     *
     * @param role the path role to look up
     * @return an optional containing the matching path, or empty if none found
     */
    public Optional<ExecutionPath> getPath(ExecutionPath.PathRole role) {
        return paths.stream()
            .filter(p -> p.getRole() == role)
            .findFirst();
    }

    /**
     * Checks whether this plan contains a path with the given role.
     *
     * @param role the path role to check for
     * @return {@code true} if a matching path exists
     */
    public boolean hasPath(ExecutionPath.PathRole role) {
        return paths.stream().anyMatch(p -> p.getRole() == role);
    }

    /**
     * Builder for constructing a QueryPlan.
     */
    public static class Builder {
        private final List<ExecutionPath> paths = new ArrayList<>();

        /** Creates a new empty Builder. */
        public Builder() {}

        /**
         * Adds an execution path to the plan being built.
         *
         * @param path the execution path to add
         * @return this builder for chaining
         */
        public Builder addPath(ExecutionPath path) {
            paths.add(path);
            return this;
        }

        /**
         * Builds the {@link QueryPlan}. At least one execution path must have been added.
         *
         * @return the constructed query plan
         * @throws IllegalStateException if no paths were added
         */
        public QueryPlan build() {
            if (paths.isEmpty()) {
                throw new IllegalStateException("QueryPlan must have at least one execution path");
            }
            return new QueryPlan(paths);
        }
    }
}

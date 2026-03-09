/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Container for one or more {@link QueryPlan}s produced by DSL-to-RelNode conversion.
 *
 * A single DSL query may require multiple independent RelNode trees when the results
 * cannot be represented in one tree (e.g., aggregation with size &gt; 0 needs both
 * a HITS plan and an AGGREGATION plan).
 */
public final class QueryPlans {

    /**
     * Identifies what part of the SearchResponse a query plan populates.
     */
    public enum Type {
        /** Document hits (query results with pagination). */
        HITS,
        /** Aggregation results computed over filtered data. */
        AGGREGATION,
    }

    /**
     * A single query plan within {@link QueryPlans}, pairing a Calcite RelNode
     * with its type for constructing the final SearchResponse.
     */
    public record QueryPlan(Type type, RelNode relNode) {}

    private final List<QueryPlan> plans;

    private QueryPlans(List<QueryPlan> plans) {
        this.plans = List.copyOf(plans);
    }

    /**
     * Returns all query plans.
     *
     * @return an unmodifiable list of query plans
     */
    public List<QueryPlan> getAll() {
        return plans;
    }

    /**
     * Returns the first query plan matching the given type, if any.
     *
     * @param type the type to look up
     * @return an optional containing the matching query plan, or empty if none found
     */
    public Optional<QueryPlan> get(Type type) {
        return plans.stream()
            .filter(p -> p.type() == type)
            .findFirst();
    }

    /**
     * Checks whether this container has a query plan with the given type.
     *
     * @param type the type to check for
     * @return {@code true} if a matching query plan exists
     */
    public boolean has(Type type) {
        return plans.stream().anyMatch(p -> p.type() == type);
    }

    /**
     * Builder for constructing QueryPlans.
     */
    public static class Builder {
        private final List<QueryPlan> plans = new ArrayList<>();

        /** Creates a new empty Builder. */
        public Builder() {}

        /**
         * Adds a query plan.
         *
         * @param plan the query plan to add
         */
        public void add(QueryPlan plan) {
            plans.add(plan);
        }

        /**
         * Builds the {@link QueryPlans}. At least one query plan must have been added.
         *
         * @return the constructed query plans
         * @throws IllegalStateException if no plans were added
         */
        public QueryPlans build() {
            if (plans.isEmpty()) {
                throw new IllegalStateException("QueryPlans must have at least one query plan");
            }
            return new QueryPlans(plans);
        }
    }
}

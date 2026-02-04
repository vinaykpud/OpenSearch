/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;

/**
 * Convention for OpenSearch physical operators.
 *
 * <p>In Calcite's optimization framework, a Convention represents a "calling convention" -
 * it describes how operators in a plan communicate and execute. Different conventions
 * represent different execution engines:
 * <ul>
 *   <li>{@code Convention.NONE} - Logical operators (no physical implementation)</li>
 *   <li>{@code EnumerableConvention} - Calcite's built-in Java iterator execution</li>
 *   <li>{@code JdbcConvention} - Push execution to a JDBC database</li>
 *   <li>{@code OpenSearchConvention} - Our physical operators that execute on OpenSearch</li>
 * </ul>
 *
 * <p>The optimizer uses conventions to:
 * <ol>
 *   <li>Track which operators can work together (same convention)</li>
 *   <li>Know when to insert adapters (different conventions)</li>
 *   <li>Guide the search for physical implementations</li>
 * </ol>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * // Request physical plan in our convention
 * RelTraitSet desiredTraits = logicalPlan.getTraitSet()
 *     .replace(OpenSearchConvention.INSTANCE);
 * planner.setRoot(planner.changeTraits(logicalPlan, desiredTraits));
 * }</pre>
 */
public class OpenSearchConvention extends Convention.Impl {

    /**
     * Singleton instance of the OpenSearch convention.
     */
    public static final OpenSearchConvention INSTANCE = new OpenSearchConvention();

    private OpenSearchConvention() {
        super("OPENSEARCH", OpenSearchRel.class);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // Rules will be registered separately via PhysicalOptimizer
    }

    @Override
    public String toString() {
        return getName();
    }
}

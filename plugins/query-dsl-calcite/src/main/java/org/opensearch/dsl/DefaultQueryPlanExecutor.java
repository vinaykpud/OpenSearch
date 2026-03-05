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
 * Placeholder executor that returns empty results.
 * A real execution engine will be plugged in later.
 */
public class DefaultQueryPlanExecutor implements QueryPlanExecutor {
    @Override
    public Object[][] execute(RelNode relNode) throws Exception {
        return new Object[0][];
    }
}

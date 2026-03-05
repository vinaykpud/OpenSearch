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
 * Executes a Calcite logical plan and returns tabular results.
 */
public interface QueryPlanExecutor {
    Object[][] execute(RelNode relNode) throws Exception;
}

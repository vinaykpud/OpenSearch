/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.opensearch.queryplanner.physical.exec.ExecNode;

/**
 * Marker interface for all OpenSearch physical operators.
 *
 * <p>This interface serves two purposes:
 * <ol>
 *   <li>Identifies RelNodes that belong to our physical convention</li>
 *   <li>Provides the bridge to our ExecNode execution layer via {@link #toExecNode()}</li>
 * </ol>
 *
 * <h2>Operator Hierarchy:</h2>
 * <pre>
 * Calcite RelNode (abstract)
 *     │
 *     ├── LogicalTableScan (Convention.NONE)
 *     │
 *     └── OpenSearchScan implements OpenSearchRel (OpenSearchConvention)
 *               │
 *               └── toExecNode() → ExecScan
 * </pre>
 *
 * <h2>Conversion Flow:</h2>
 * <pre>
 * LogicalRelNode (planning) → OpenSearchRel (physical) → ExecNode (execution)
 * </pre>
 *
 * <p>All implementations must:
 * <ul>
 *   <li>Return {@link OpenSearchConvention#INSTANCE} from getConvention()</li>
 *   <li>Implement toExecNode() to convert to the execution representation</li>
 * </ul>
 */
public interface OpenSearchRel extends RelNode {

    /**
     * Convert this physical RelNode to an ExecNode for execution.
     *
     * <p>The ExecNode tree mirrors the RelNode tree structure but:
     * <ul>
     *   <li>Is Writeable (can be serialized for transport)</li>
     *   <li>Has no Calcite dependencies</li>
     *   <li>Can be executed by OperatorFactory</li>
     * </ul>
     *
     * @return ExecNode representation of this operator
     */
    ExecNode toExecNode();

    /**
     * Get the convention for OpenSearch operators.
     */
    default Convention getConvention() {
        return OpenSearchConvention.INSTANCE;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner;

import org.apache.calcite.jdbc.CalciteSchema;

/**
 * Interface for providing table schemas to the query planner.
 */
public interface SchemaProvider {
    /**
     * Register tables into the Calcite schema.
     */
    void registerTables(CalciteSchema rootSchema);
}

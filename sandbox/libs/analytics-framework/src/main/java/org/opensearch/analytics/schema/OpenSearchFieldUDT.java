/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

/**
 * Semantic tags for OpenSearch field types that need custom Calcite type
 * handling. Each is backed by VARCHAR in Calcite but carries semantic
 * meaning for downstream converters (e.g., Substrait type mapping).
 */
public enum OpenSearchFieldUDT {
    TIMESTAMP,
    TIMESTAMP_NANOS,
    IP,
    BINARY
}

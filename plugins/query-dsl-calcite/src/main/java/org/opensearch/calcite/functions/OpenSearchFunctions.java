/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.functions;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Custom SQL functions for OpenSearch-specific operations in Calcite.
 * 
 * These functions represent OpenSearch query capabilities that don't have
 * direct SQL equivalents, such as full-text search.
 */
public class OpenSearchFunctions {
    
    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private OpenSearchFunctions() {
        // Utility class - no instantiation
    }
    
    /**
     * MATCH_QUERY(field, text, operator) - Full-text match query.
     * 
     * This function represents OpenSearch's match query, which performs
     * full-text search with analysis and scoring.
     * 
     * Parameters:
     * - field: The field name to search (STRING)
     * - text: The text to search for (STRING)
     * - operator: The operator ("AND" or "OR") (STRING)
     * 
     * Returns: BOOLEAN (true if document matches)
     */
    public static final SqlFunction MATCH_QUERY = new SqlFunction(
        "MATCH_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
}

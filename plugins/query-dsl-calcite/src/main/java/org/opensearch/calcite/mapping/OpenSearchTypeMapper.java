/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.mapping;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Maps OpenSearch field types to Calcite SqlTypeName.
 *
 * Provides a direct mapping from OpenSearch index mapping types to standard
 * Calcite SQL types. Type names are normalized (underscores removed, lowercased)
 * before matching. Unknown types default to {@link SqlTypeName#ANY}.
 */
public class OpenSearchTypeMapper {

    /**
     * Enum representing OpenSearch field types and their Calcite equivalents.
     * 
     * Based on OpenSearch mapping types:
     * https://opensearch.org/docs/latest/field-types/
     */
    public enum MappingType {
        /** OpenSearch keyword type - exact value matching */
        KEYWORD("keyword", SqlTypeName.VARCHAR),
        /** OpenSearch text type - full-text search */
        TEXT("text", SqlTypeName.VARCHAR),
        /** OpenSearch match_only_text type - optimized for full-text search */
        MATCH_ONLY_TEXT("match_only_text", SqlTypeName.VARCHAR),
        
        /** OpenSearch long type - 64-bit signed integer */
        LONG("long", SqlTypeName.BIGINT),
        /** OpenSearch integer type - 32-bit signed integer */
        INTEGER("integer", SqlTypeName.INTEGER),
        /** OpenSearch short type - 16-bit signed integer */
        SHORT("short", SqlTypeName.SMALLINT),
        /** OpenSearch byte type - 8-bit signed integer */
        BYTE("byte", SqlTypeName.TINYINT),
        /** OpenSearch double type - 64-bit floating point */
        DOUBLE("double", SqlTypeName.DOUBLE),
        /** OpenSearch float type - 32-bit floating point */
        FLOAT("float", SqlTypeName.FLOAT),
        /** OpenSearch half_float type - 16-bit floating point */
        HALF_FLOAT("half_float", SqlTypeName.FLOAT),
        /** OpenSearch scaled_float type - floating point with scaling factor */
        SCALED_FLOAT("scaled_float", SqlTypeName.DOUBLE),  // SQL plugin uses DOUBLE
        
        /** OpenSearch boolean type */
        BOOLEAN("boolean", SqlTypeName.BOOLEAN),
        
        /** OpenSearch date type - date/time with millisecond precision */
        DATE("date", SqlTypeName.TIMESTAMP),
        /** OpenSearch date_nanos type - date/time with nanosecond precision */
        DATE_NANOS("date_nanos", SqlTypeName.TIMESTAMP),
        
        /** OpenSearch binary type - base64 encoded binary data */
        BINARY("binary", SqlTypeName.VARBINARY),
        
        /** OpenSearch ip type - IPv4 or IPv6 address */
        IP("ip", SqlTypeName.VARCHAR),
        /** OpenSearch completion type - auto-complete suggestions */
        COMPLETION("completion", SqlTypeName.VARCHAR),
        /** OpenSearch token_count type - count of tokens in a field */
        TOKEN_COUNT("token_count", SqlTypeName.INTEGER),
        
        /** OpenSearch object type - JSON object with properties */
        OBJECT("object", SqlTypeName.ANY),
        /** OpenSearch nested type - array of objects */
        NESTED("nested", SqlTypeName.ANY),
        /** OpenSearch geo_point type - latitude/longitude point */
        GEO_POINT("geo_point", SqlTypeName.ANY),
        /** OpenSearch geo_shape type - complex geographic shapes */
        GEO_SHAPE("geo_shape", SqlTypeName.ANY),
        /** OpenSearch alias type - reference to another field */
        ALIAS("alias", SqlTypeName.ANY);

        private final String opensearchType;
        private final SqlTypeName calciteType;

        MappingType(String opensearchType, SqlTypeName calciteType) {
            this.opensearchType = opensearchType;
            this.calciteType = calciteType;
        }

        /**
         * Gets the OpenSearch type name.
         * 
         * @return The OpenSearch type string
         */
        public String getOpensearchType() {
            return opensearchType;
        }

        /**
         * Gets the corresponding Calcite SqlTypeName.
         * 
         * @return The Calcite type
         */
        public SqlTypeName getCalciteType() {
            return calciteType;
        }
    }

    /**
     * Converts an OpenSearch field type string to a Calcite SqlTypeName.
     * 
     * This method performs case-insensitive matching and handles underscores
     * in type names (e.g., "half_float" vs "halffloat").
     * 
     * @param opensearchType The OpenSearch field type (e.g., "keyword", "long", "date")
     * @return The corresponding Calcite SqlTypeName, or SqlTypeName.ANY for unknown types
     */
    public static SqlTypeName toCalciteType(String opensearchType) {
        if (opensearchType == null || opensearchType.isEmpty()) {
            return SqlTypeName.ANY;
        }

        // Normalize: remove underscores and convert to lowercase for comparison
        String normalized = opensearchType.replace("_", "").toLowerCase();

        // Search for matching type
        for (MappingType type : MappingType.values()) {
            String typeNormalized = type.opensearchType.replace("_", "").toLowerCase();
            if (typeNormalized.equals(normalized)) {
                return type.calciteType;
            }
        }

        // Unknown types default to ANY
        return SqlTypeName.ANY;
    }

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private OpenSearchTypeMapper() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
}

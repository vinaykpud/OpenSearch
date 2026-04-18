/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;

/**
 * Calcite type factory that supports {@link OpenSearchFieldType} UDTs.
 *
 * <p>Overrides {@link #createTypeWithNullability} so that UDT instances
 * preserve their semantic tag when nullability is toggled.
 */
public class OpenSearchTypeFactory extends SqlTypeFactoryImpl {

    public static final OpenSearchTypeFactory INSTANCE = new OpenSearchTypeFactory(RelDataTypeSystem.DEFAULT);

    public OpenSearchTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    public OpenSearchFieldType createFieldType(OpenSearchFieldUDT udt, boolean nullable) {
        return createFieldType(udt, nullable, null);
    }

    public OpenSearchFieldType createFieldType(OpenSearchFieldUDT udt, boolean nullable, String format) {
        BasicSqlType varchar = (BasicSqlType) createSqlType(SqlTypeName.VARCHAR);
        if (nullable) {
            varchar = (BasicSqlType) createTypeWithNullability(varchar, true);
        }
        return new OpenSearchFieldType(udt, varchar, format);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof OpenSearchFieldType osType) {
            return osType.withNullability(this, nullable);
        }
        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType createTypeWithCharsetAndCollation(RelDataType type, Charset charset, SqlCollation collation) {
        if (type instanceof OpenSearchFieldType) {
            return type;
        }
        return super.createTypeWithCharsetAndCollation(type, charset, collation);
    }
}

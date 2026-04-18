/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;

/**
 * A VARCHAR-backed Calcite type that carries an {@link OpenSearchFieldUDT} tag.
 *
 * <p>Because {@link #getSqlTypeName()} returns {@code VARCHAR}, Calcite's
 * {@code RexBuilder.makeLiteral()} accepts string values without coercion —
 * fixing the ClassCastException that occurs with native TIMESTAMP types.
 * The UDT tag lets downstream converters (e.g., Substrait) identify the
 * semantic type and map it appropriately.
 */
public class OpenSearchFieldType extends RelDataTypeImpl {

    private final OpenSearchFieldUDT udt;
    private final BasicSqlType backingType;
    private final @Nullable String format;

    OpenSearchFieldType(OpenSearchFieldUDT udt, BasicSqlType backingType, @Nullable String format) {
        this.udt = udt;
        this.backingType = backingType;
        this.format = format;
        computeDigest();
    }

    public OpenSearchFieldUDT getUdt() {
        return udt;
    }

    public @Nullable String getFormat() {
        return format;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return backingType.getSqlTypeName();
    }

    @Override
    public boolean isNullable() {
        return backingType.isNullable();
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return backingType.getFamily();
    }

    @Override
    public @Nullable Charset getCharset() {
        return backingType.getCharset();
    }

    @Override
    public @Nullable SqlCollation getCollation() {
        return backingType.getCollation();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("OS_").append(udt.name());
        if (format != null) {
            sb.append("[").append(format).append("]");
        }
    }

    OpenSearchFieldType withNullability(OpenSearchTypeFactory typeFactory, boolean nullable) {
        if (isNullable() == nullable) {
            return this;
        }
        BasicSqlType newBacking = (BasicSqlType) typeFactory.createTypeWithNullability(backingType, nullable);
        return new OpenSearchFieldType(udt, newBacking, format);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code make_struct('name0', value0, 'name1', value1, ...)} → ROW.
 *
 * <p>Materializes an OpenSearch {@code object} field as a struct from the flat dotted leaf
 * columns the parquet scan produces. The engine stores {@code object} sub-fields as flat
 * columns ({@code a.b.c}); this function re-assembles them into the nested shape at query
 * time so a projection or aggregate can address the object as a single value. Nested objects
 * nest the call:
 *
 * <pre>
 * make_struct('top', $1, 'properties', make_struct('name', $2, 'value', $3))
 * </pre>
 *
 * <p>Executes as DataFusion's {@code named_struct}, which takes the same
 * (name, value, name, value, …) calling convention. The call does <em>not</em> reach DataFusion
 * through Substrait's function-signature matching: {@code MakeStructCallConverter} intercepts it
 * and builds the invocation directly, because isthmus cannot match a variadic function whose
 * operands are deliberately of differing types. That is what makes the number of struct fields
 * unbounded.
 *
 * <p>The return type is always supplied explicitly by the caller via
 * {@link #makeCall(RexBuilder, RelDataType, List, List)} — the operand-driven inference is a
 * placeholder ({@code ANY}) because the authoritative ROW type comes from the index mapping,
 * not from the operands.
 *
 * @opensearch.internal
 */
public final class MakeStructFunction {

    /** The function name used in Calcite plans and Substrait serialization. */
    public static final String NAME = "make_struct";

    /** Singleton Calcite SqlFunction: {@code make_struct(VARCHAR, ANY, ...) → ROW}. */
    public static final SqlFunction FUNCTION = new SqlFunction(
        NAME,
        SqlKind.OTHER_FUNCTION,
        // Placeholder: callers always construct with an explicit ROW type (see makeCall).
        opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private MakeStructFunction() {}

    /**
     * Builds {@code make_struct('f0', v0, 'f1', v1, ...)} with an explicit ROW return type.
     *
     * @param rexBuilder  builder for the enclosing plan
     * @param structType  the ROW type this call produces (from the index mapping)
     * @param fieldNames  struct field names, in order
     * @param fieldValues struct field value expressions, positionally paired with {@code fieldNames}
     */
    public static RexNode makeCall(
        RexBuilder rexBuilder,
        RelDataType structType,
        List<String> fieldNames,
        List<RexNode> fieldValues
    ) {
        if (fieldNames.size() != fieldValues.size()) {
            throw new IllegalArgumentException(
                "make_struct requires one value per field name; got " + fieldNames.size() + " names and " + fieldValues.size() + " values"
            );
        }
        // Field names are VARCHAR, not CHAR. rexBuilder.makeLiteral(String) would produce a
        // CHAR(n) literal, which Substrait types as the fixed-width `char<n>`; DataFusion's
        // named_struct expects a variable-length string for a field name, so VARCHAR (→ Substrait
        // `string`) is the faithful type. It also keeps the literal free of the padding semantics
        // CHAR carries.
        RelDataType nameType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        List<RexNode> operands = new ArrayList<>(fieldNames.size() * 2);
        for (int i = 0; i < fieldNames.size(); i++) {
            operands.add(rexBuilder.makeLiteral(fieldNames.get(i), nameType, true));
            operands.add(fieldValues.get(i));
        }
        return rexBuilder.makeCall(structType, FUNCTION, operands);
    }
}

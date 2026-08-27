/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.MakeStructFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;

/**
 * Converts a {@code make_struct} / {@code named_struct} call into a Substrait
 * {@link Expression.ScalarFunctionInvocation} built <em>directly</em>, bypassing isthmus'
 * function-signature matching. This is what lifts the arity ceiling on struct construction.
 *
 * <h2>Why the signature matcher can't do this</h2>
 *
 * <ul>
 *   <li><b>A variadic declaration never matches.</b> isthmus binds variadic functions through a
 *       {@code SingularArgumentMatcher} — it derives ONE type every operand must satisfy.
 *       {@code named_struct} interleaves {@code string} field names with values of unrelated
 *       types, so no such type exists. Verified against isthmus 0.89.1: neither
 *       {@code value: any1} nor unconstrained {@code value: any} (both with
 *       {@code parameterConsistency: INCONSISTENT}) matches — each still fails with
 *       "Unable to convert call named_struct(...)". Same reason {@link MakeArrayAdapter} must
 *       widen {@code make_array}'s operands to a common element type first.</li>
 *   <li><b>Fixed-arity enumeration is bounded.</b> One impl per field count does match, but an
 *       object's width is data-dependent: an OTel span's {@code attributes} carries ~55
 *       sub-fields and grows as new attribute keys appear.</li>
 * </ul>
 *
 * <p>{@code RexExpressionConverter.visitCall} offers a call to every registered
 * {@link CallConverter} and throws "Unable to convert call ..." only once all of them decline, so
 * constructing the invocation here skips the matcher entirely. The declaration is used purely as
 * the extension <em>anchor</em> (name + URN) that the consumer resolves by name; the argument list
 * we attach is the real one, so its length is unconstrained by the declared variant.
 *
 * <h2>Why not a nested-struct expression</h2>
 *
 * Substrait models struct construction natively as {@code Expression.NestedStruct}, which would be
 * the cleaner representation, but DataFusion's substrait consumer rejects it at execution:
 * {@code "This feature is not implemented: Nested struct expressions are not yet supported"}.
 * DataFusion does implement {@code named_struct} as a scalar function at any arity, so the
 * function-invocation form is the one that actually executes. Revisit if that gap is closed
 * upstream.
 *
 * <p>Operands are forwarded unchanged — including the field-name literals — because DataFusion's
 * {@code named_struct} takes the interleaved {@code (name, value, …)} form.
 *
 * @opensearch.internal
 */
class MakeStructCallConverter implements CallConverter {

    /** DataFusion's native struct constructor — the name the consumer resolves. */
    static final String NAMED_STRUCT = "named_struct";

    private final SimpleExtension.ExtensionCollection extensions;
    private final TypeConverter typeConverter;

    MakeStructCallConverter(SimpleExtension.ExtensionCollection extensions, TypeConverter typeConverter) {
        this.extensions = extensions;
        this.typeConverter = typeConverter;
    }

    @Override
    public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        String operator = call.getOperator().getName();
        // The engine emits `make_struct`; `named_struct` is accepted too so the converter stays
        // correct if a rename ever reaches it first.
        if (!MakeStructFunction.NAME.equalsIgnoreCase(operator) && !NAMED_STRUCT.equalsIgnoreCase(operator)) {
            return Optional.empty();
        }

        Optional<SimpleExtension.ScalarFunctionVariant> declaration = findNamedStructDeclaration();
        if (declaration.isEmpty()) {
            // No anchor to reference — decline so the failure surfaces as isthmus' normal
            // "Unable to convert call" rather than an NPE deep in proto serialization.
            return Optional.empty();
        }

        List<FunctionArg> arguments = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            arguments.add(topLevelConverter.apply(operand));
        }

        return Optional.of(
            Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration.get())
                .addAllArguments(arguments)
                .outputType(typeConverter.toSubstrait(call.getType()))
                .build()
        );
    }

    /**
     * Any declared {@code named_struct} variant works as the anchor — the consumer resolves the
     * function by name, and the arity we attach is independent of the variant's declared arity.
     */
    private Optional<SimpleExtension.ScalarFunctionVariant> findNamedStructDeclaration() {
        return extensions.scalarFunctions().stream().filter(variant -> NAMED_STRUCT.equals(variant.name())).findFirst();
    }
}

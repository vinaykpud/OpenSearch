/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.schema.OpenSearchFieldType;
import org.opensearch.analytics.schema.OpenSearchFieldUDT;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DataFusion fragment converter. Converts stripped Calcite RelNode fragments
 * to Substrait plan bytes that the DataFusion Rust runtime can consume.
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private static final String DEFAULT_DATE_FORMAT =
        "strict_date_time_no_millis||strict_date_optional_time||epoch_millis";

    private static volatile SimpleExtension.ExtensionCollection EXTENSIONS;

    private static SimpleExtension.ExtensionCollection getExtensions() {
        if (EXTENSIONS == null) {
            synchronized (DataFusionFragmentConvertor.class) {
                if (EXTENSIONS == null) {
                    ClassLoader original = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(DataFusionFragmentConvertor.class.getClassLoader());
                        EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION;
                    } finally {
                        Thread.currentThread().setContextClassLoader(original);
                    }
                }
            }
        }
        return EXTENSIONS;
    }

    @Override
    public byte[] convertScanFragment(String tableName, RelNode fragment) {
        LOGGER.info("Converting scan fragment for table [{}]:\n{}", tableName, RelOptUtil.toString(fragment));

        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(field -> field.getValue())
            .toList();

        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        Plan plan = Plan.builder().addRoots(substraitRoot).build();
        plan = new SubstraitPlanRewriter(fragment).rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.info("Substrait plan: {} bytes, proto:\n{}", bytes.length, protoPlan);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = new TypeConverter(new UserTypeMapper() {
            @Override
            public Type toSubstrait(RelDataType relDataType) {
                if (!(relDataType instanceof OpenSearchFieldType osType)) return null;
                boolean nullable = relDataType.isNullable();
                return switch (osType.getUdt()) {
                    case TIMESTAMP -> Type.withNullability(nullable).precisionTimestamp(3);
                    case TIMESTAMP_NANOS -> Type.withNullability(nullable).precisionTimestamp(9);
                    case IP, BINARY -> Type.withNullability(nullable).BINARY;
                };
            }

            @Override
            public RelDataType toCalcite(Type.UserDefined type) {
                return null;
            }
        });

        SimpleExtension.ExtensionCollection extensions = getExtensions();
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(), typeFactory
        );
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(), List.of(), typeFactory, typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(), typeFactory
        );

        return new SubstraitRelVisitor(
            typeFactory, scalarConverter, aggConverter, windowConverter,
            typeConverter, ImmutableFeatureBoard.builder().build()
        );
    }

    /**
     * Single-pass Substrait plan rewriter that handles:
     * 1. Stripping Calcite catalog prefix from NamedScan table names
     * 2. Rewriting date EQUALS to range predicates with precision_timestamp(3) literals
     */
    private static class SubstraitPlanRewriter {
        private final RelDataType rowType;

        SubstraitPlanRewriter(RelNode calciteFragment) {
            this.rowType = resolveInputRowType(calciteFragment);
        }

        private static RelDataType resolveInputRowType(RelNode node) {
            if (node.getInputs().size() == 1) {
                return node.getInput(0).getRowType();
            }
            return node.getRowType();
        }

        Plan rewrite(Plan plan) {
            RelCopyOnWriteVisitor<RuntimeException> visitor = new RelCopyOnWriteVisitor<>() {
                // Strip Calcite catalog prefix from table names so DataFusion sees just the index name
                @Override
                public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext ctx) {
                    List<String> names = namedScan.getNames();
                    if (names.size() > 1) {
                        return Optional.of(NamedScan.builder().from(namedScan)
                            .names(List.of(names.get(names.size() - 1))).build());
                    }
                    return super.visit(namedScan, ctx);
                }

                // Rewrite date EQUALS to range with precision_timestamp(3) literals
                @Override
                public Optional<Rel> visit(Filter filter, EmptyVisitationContext ctx) {
                    Optional<Rel> newInput = filter.getInput().accept(this, ctx);
                    Optional<Expression> newCond = rewriteDateEquals(filter.getCondition());
                    if (newInput.isEmpty() && newCond.isEmpty()) return Optional.empty();
                    return Optional.of(Filter.builder().from(filter)
                        .input(newInput.orElse(filter.getInput()))
                        .condition(newCond.orElse(filter.getCondition())).build());
                }
            };

            List<Plan.Root> roots = new ArrayList<>();
            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> modified = root.getInput().accept(visitor, null);
                if (modified.isPresent()) {
                    roots.add(Plan.Root.builder().from(root).input(modified.get()).build());
                } else {
                    roots.add(root);
                }
            }
            return Plan.builder().from(plan).roots(roots).build();
        }

        private Optional<Expression> rewriteDateEquals(Expression expr) {
            if (!(expr instanceof Expression.ScalarFunctionInvocation sfi)) return Optional.empty();
            String name = sfi.declaration().name();

            if ("and".equals(name) || "or".equals(name)) {
                boolean changed = false;
                List<FunctionArg> args = new ArrayList<>();
                for (FunctionArg arg : sfi.arguments()) {
                    if (arg instanceof Expression e) {
                        Optional<Expression> r = rewriteDateEquals(e);
                        args.add(r.orElse(e));
                        if (r.isPresent()) changed = true;
                    } else {
                        args.add(arg);
                    }
                }
                return changed ? Optional.of(Expression.ScalarFunctionInvocation.builder()
                    .from(sfi).arguments(args).build()) : Optional.empty();
            }

            if (!name.startsWith("equal") || sfi.arguments().size() != 2) return Optional.empty();

            FieldReference fieldRef = null;
            String dateValue = null;
            int fieldIndex = -1;
            for (FunctionArg arg : sfi.arguments()) {
                Expression unwrapped = arg instanceof Expression.Cast c ? c.input() : (Expression) arg;
                if (unwrapped instanceof FieldReference fr && fr.isSimpleRootReference()
                    && fr.segments().get(0) instanceof FieldReference.StructField sf) {
                    fieldRef = fr;
                    fieldIndex = sf.offset();
                } else if (unwrapped instanceof Expression.StrLiteral s) {
                    dateValue = s.value();
                }
            }
            if (fieldRef == null || dateValue == null || fieldIndex < 0
                || fieldIndex >= rowType.getFieldCount()) return Optional.empty();

            RelDataTypeField field = rowType.getFieldList().get(fieldIndex);
            if (!(field.getType() instanceof OpenSearchFieldType osType)) return Optional.empty();
            OpenSearchFieldUDT udt = osType.getUdt();
            if (udt != OpenSearchFieldUDT.TIMESTAMP && udt != OpenSearchFieldUDT.TIMESTAMP_NANOS) {
                return Optional.empty();
            }

            String format = osType.getFormat() != null ? osType.getFormat() : DEFAULT_DATE_FORMAT;
            DateMathParser parser = DateFormatter.forPattern(format).toDateMathParser();
            long now = System.currentTimeMillis();
            long lowerMs = parser.parse(dateValue, () -> now, false, ZoneOffset.UTC).toEpochMilli();
            long upperMs = parser.parse(dateValue, () -> now, true, ZoneOffset.UTC).toEpochMilli();

            int precision = udt == OpenSearchFieldUDT.TIMESTAMP_NANOS ? 9 : 3;
            long lowerVal = precision == 9 ? lowerMs * 1_000_000 : lowerMs;
            long upperVal = precision == 9 ? upperMs * 1_000_000 : upperMs;

            Type bool = Type.withNullability(true).BOOLEAN;
            SimpleExtension.ExtensionCollection ext = getExtensions();
            Expression gte = scalarCall(ext, DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
                "gte:any_any", bool, fieldRef, ptsLiteral(lowerVal, precision));
            Expression lte = scalarCall(ext, DefaultExtensionCatalog.FUNCTIONS_COMPARISON,
                "lte:any_any", bool, fieldRef, ptsLiteral(upperVal, precision));
            return Optional.of(scalarCall(ext, DefaultExtensionCatalog.FUNCTIONS_BOOLEAN,
                "and:bool", bool, gte, lte));
        }

        private static Expression ptsLiteral(long value, int precision) {
            return ImmutableExpression.PrecisionTimestampLiteral.builder()
                .value(value).precision(precision).nullable(false).build();
        }

        private static Expression scalarCall(SimpleExtension.ExtensionCollection ext,
                String uri, String key, Type outputType, Expression... args) {
            return Expression.ScalarFunctionInvocation.builder()
                .declaration(ext.getScalarFunction(SimpleExtension.FunctionAnchor.of(uri, key)))
                .addArguments(args).outputType(outputType).build();
        }
    }
}

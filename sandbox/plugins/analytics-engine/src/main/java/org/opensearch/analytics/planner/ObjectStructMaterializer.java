/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.MakeStructFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Materializes OpenSearch {@code object} fields into structs in a project directly above the
 * table scan.
 *
 * <p>The engine stores an {@code object} mapping as flat dotted leaf columns
 * ({@code nested_metadata.top}, {@code nested_metadata.properties.name}, …); the schema also
 * exposes the object itself as a struct-typed (ROW) column so a query can name it — see
 * {@code OpenSearchSchemaBuilder.addLeafFields}. The object has no <em>physical</em> column
 * though: {@code FieldStorageResolver.populateFromProperties} recurses past object parents
 * precisely because "object fields themselves have no storage". So the scan must not be asked
 * to produce it.
 *
 * <p>This rewriter therefore does two things at once — it removes struct columns from the scan
 * and re-assembles them in a project above it, at their original positions:
 *
 * <pre>
 * LogicalProject(id=[$0], nested_metadata.top=[$1], nested_metadata.properties.name=[$2],
 *                nested_metadata.properties.value=[$3],
 *                nested_metadata=[make_struct('top', $1,
 *                                   'properties', make_struct('name', $2, 'value', $3))])
 *   LogicalTableScan(table=[[t]])      // row type: leaves only, no nested_metadata
 * </pre>
 *
 * <p>The project reproduces the scan's original row type <em>exactly</em> — same field names,
 * order, and types — so every {@code RexInputRef} in the operators above stays valid and no
 * upstream rewriting is needed. Consequently an aggregate over an object
 * ({@code stats … by nested_metadata}) receives an already-materialized struct, and a projection
 * of the object returns the whole object.
 *
 * <p>A sub-object nests another {@code make_struct} over its own leaves, so arbitrarily deep
 * {@code object} trees materialize in one pass.
 *
 * <p>Runs as a one-shot top-down pass (like {@code OpenSearchTopKRewriter}) rather than a HEP
 * rule: a rule matching {@code TableScan} and producing {@code Project(TableScan)} would re-match
 * its own output and never reach fixpoint.
 *
 * <p>Placement matters, and it is <em>before</em> {@code trimFields} — see the call site in
 * {@code PlannerImpl.runAllOptimizations}. This pass emits one {@code make_struct} per object
 * column the scan declares, and the field trimmer then removes the ones a given query never
 * references; without that, a query filtering only on a leaf would pay for (and could fail on) an
 * unrelated object. Leaf predicates still push down afterwards, because the leaves remain in this
 * project's output and {@code FILTER_PROJECT_TRANSPOSE} moves filters through it.
 *
 * <p>If predicates authored against the struct itself ever need to push down, the companion
 * rewrite is {@code GET_FIELD(struct, 'x') → leaf ref}.
 *
 * @opensearch.internal
 */
public final class ObjectStructMaterializer {

    private ObjectStructMaterializer() {}

    /**
     * Rewrites scans that expose struct-typed columns into a leaf-only scan plus a
     * struct-materializing project.
     *
     * @return the rewritten plan, or {@link Optional#empty()} when the plan has no object
     *         columns (callers keep the original plan unchanged)
     */
    public static Optional<RelNode> rewrite(RelNode root) {
        Materializer materializer = new Materializer();
        RelNode rewritten = root.accept(materializer);
        return materializer.changed ? Optional.of(rewritten) : Optional.empty();
    }

    private static final class Materializer extends RelShuttleImpl {

        private boolean changed = false;

        @Override
        public RelNode visit(TableScan scan) {
            RelDataType originalRowType = scan.getRowType();
            List<RelDataTypeField> originalFields = originalRowType.getFieldList();
            if (originalFields.stream().noneMatch(f -> f.getType().isStruct())) {
                return scan;
            }

            // The scan keeps only physically-stored (non-struct) columns.
            RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
            RelDataTypeFactory.Builder leafTypeBuilder = typeFactory.builder();
            Map<String, Integer> leafIndexByName = new HashMap<>();
            for (RelDataTypeField field : originalFields) {
                if (field.getType().isStruct()) {
                    continue;
                }
                leafIndexByName.put(field.getName(), leafTypeBuilder.getFieldCount());
                leafTypeBuilder.add(field.getName(), field.getType());
            }
            RelDataType leafRowType = leafTypeBuilder.build();
            if (leafRowType.getFieldCount() == 0) {
                // Nothing physical to read (an object-only projection). Leave the plan alone
                // rather than emit a scan with an empty row type.
                return scan;
            }

            RelOptTable leafTable = new LeafOnlyTable(scan.getTable(), leafRowType);
            RelNode leafScan = LogicalTableScan.create(scan.getCluster(), leafTable, scan.getHints());

            // Rebuild the ORIGINAL row type above the trimmed scan: pass leaves through and
            // assemble each struct in place, so parent input refs keep their meaning.
            RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            List<RexNode> projects = new ArrayList<>(originalFields.size());
            List<String> names = new ArrayList<>(originalFields.size());
            for (RelDataTypeField field : originalFields) {
                names.add(field.getName());
                if (field.getType().isStruct()) {
                    RexNode struct = buildStruct(rexBuilder, leafScan, field.getName(), field.getType(), leafIndexByName);
                    if (struct == null) {
                        // A backing leaf is absent (e.g. an unsupported sub-field type was dropped
                        // from the schema). Emit a typed NULL rather than a partial struct, keeping
                        // the row type stable for the operators above.
                        projects.add(rexBuilder.makeNullLiteral(field.getType()));
                    } else {
                        projects.add(struct);
                    }
                } else {
                    projects.add(rexBuilder.makeInputRef(leafScan, leafIndexByName.get(field.getName())));
                }
            }

            changed = true;
            return LogicalProject.create(leafScan, List.of(), projects, names);
        }

        /**
         * Recursively builds {@code make_struct} for {@code structType}, resolving each leaf to the
         * trimmed scan's column named {@code path + "." + fieldName}. Returns {@code null} when any
         * leaf is missing, signaling the caller to skip materialization for this column.
         */
        private static RexNode buildStruct(
            RexBuilder rexBuilder,
            RelNode leafScan,
            String path,
            RelDataType structType,
            Map<String, Integer> leafIndexByName
        ) {
            List<String> fieldNames = new ArrayList<>();
            List<RexNode> fieldValues = new ArrayList<>();
            for (RelDataTypeField child : structType.getFieldList()) {
                String childPath = path + "." + child.getName();
                RexNode value;
                if (child.getType().isStruct()) {
                    // A child that is itself an object nests another make_struct over its own leaves.
                    value = buildStruct(rexBuilder, leafScan, childPath, child.getType(), leafIndexByName);
                } else {
                    Integer leafIndex = leafIndexByName.get(childPath);
                    value = leafIndex == null ? null : rexBuilder.makeInputRef(leafScan, leafIndex);
                }
                if (value == null) {
                    return null;
                }
                fieldNames.add(child.getName());
                fieldValues.add(value);
            }
            if (fieldNames.isEmpty()) {
                return null;
            }
            return MakeStructFunction.makeCall(rexBuilder, structType, fieldNames, fieldValues);
        }
    }

    /**
     * Wraps the scanned table with a row type stripped of struct columns, so downstream physical
     * resolution ({@code FieldStorageResolver}) only ever sees fields that actually have storage.
     * Mirrors the {@code IndexNameTable} wrapper in {@code OpenSearchTableScanRule}.
     */
    private static final class LeafOnlyTable extends RelOptAbstractTable {

        private final RelOptTable delegate;

        LeafOnlyTable(RelOptTable delegate, RelDataType leafRowType) {
            super(delegate.getRelOptSchema(), delegate.getQualifiedName().getLast(), leafRowType);
            this.delegate = delegate;
        }

        @Override
        public List<String> getQualifiedName() {
            // Preserve the original qualified name: OpenSearchTableScanRule resolves the index
            // from it, and RelOptAbstractTable would otherwise report a single-segment name.
            return delegate.getQualifiedName();
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            T unwrapped = super.unwrap(clazz);
            return unwrapped != null ? unwrapped : delegate.unwrap(clazz);
        }
    }
}

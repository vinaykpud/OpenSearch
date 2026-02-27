/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.calcite.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for producing annotated RelNode explain output that resolves
 * {@code $N} field references to their actual field names.
 *
 * <p>Standard Calcite explain output uses positional references like {@code $0}, {@code $8}
 * which are hard to read for schemas with many fields. This utility appends the field name
 * after each reference, e.g. {@code $8:price}, making plans much easier to understand.</p>
 */
public final class RelNodeExplainUtils {

    private static final Pattern FIELD_REF_PATTERN = Pattern.compile("\\$(\\d+)");

    private RelNodeExplainUtils() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Produces an annotated explain string for the given RelNode tree.
     * Each line is annotated using the input row type of the corresponding node,
     * resolving {@code $N} references to {@code $N:fieldName}.
     * Includes a schema legend at the end.
     *
     * <p>This is intended for human-readable console/REST output.</p>
     *
     * @param root the root RelNode of the plan
     * @return annotated multi-line explain string
     */
    public static String annotatedExplain(RelNode root) {
        return buildExplain(root, true);
    }

    /**
     * Internal method that builds the explain string with or without field name annotations.
     */
    private static String buildExplain(RelNode root, boolean annotate) {
        String plain = root.explain();
        String[] lines = plain.split("\n");
        StringBuilder sb = new StringBuilder();

        // Walk the tree in the same order explain() prints: root first, then children indented.
        // We collect nodes in explain order (pre-order DFS).
        List<RelNode> nodes = new java.util.ArrayList<>();
        collectPreOrder(root, nodes);

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (annotate && i < nodes.size()) {
                RelNode node = nodes.get(i);
                RelDataType inputRowType = resolveInputRowType(node);
                if (inputRowType != null) {
                    line = annotateFieldRefs(line, inputRowType);
                }
            }
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append(line);
        }
        return sb.toString();
    }

    /**
     * Collects RelNodes in pre-order (same order as explain() prints them).
     */
    private static void collectPreOrder(RelNode node, List<RelNode> result) {
        result.add(node);
        for (RelNode input : node.getInputs()) {
            collectPreOrder(input, result);
        }
    }

    /**
     * Determines the appropriate row type to use for resolving field references in a node.
     * For most nodes, field refs refer to the input's row type.
     * For Sort/Filter after Aggregate, refs refer to the Aggregate's output row type.
     */
    private static RelDataType resolveInputRowType(RelNode node) {
        if (node instanceof TableScan) {
            return node.getRowType();
        }
        if (node.getInputs().isEmpty()) {
            return node.getRowType();
        }
        // For Sort, Filter, Project — $N refers to the input's row type
        // But Sort after Aggregate: $N refers to Aggregate's output (which is the Sort's input)
        RelNode input = node.getInput(0);
        return input.getRowType();
    }

    /**
     * Replaces {@code $N} with {@code $N:fieldName} in the given line.
     */
    private static String annotateFieldRefs(String line, RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        Matcher matcher = FIELD_REF_PATTERN.matcher(line);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            int idx = Integer.parseInt(matcher.group(1));
            String replacement;
            if (idx >= 0 && idx < fields.size()) {
                replacement = "\\$" + idx + ":" + fields.get(idx).getName();
            } else {
                replacement = matcher.group();
            }
            matcher.appendReplacement(result, replacement);
        }
        matcher.appendTail(result);
        return result.toString();
    }
}

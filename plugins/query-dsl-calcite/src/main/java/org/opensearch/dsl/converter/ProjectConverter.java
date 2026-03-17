/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.ConversionContext;
import org.opensearch.dsl.exception.ConversionException;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies field selection as a LogicalProject.
 * Handles exact field names, wildcard patterns, and _source: false.
 */
public class ProjectConverter extends AbstractDslConverter {

    /** Creates a new ProjectConverter. */
    public ProjectConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getSearchSource().fetchSource() != null;
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalProject.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        FetchSourceContext fetchSource = ctx.getSearchSource().fetchSource();

        if (!fetchSource.fetchSource()) {
            return createEmptyProjection(input);
        }

        String[] includes = fetchSource.includes();
        if (includes == null || includes.length == 0) {
            return input;
        }

        return createIncludeProjection(input, includes, ctx.getRexBuilder());
    }

    private RelNode createEmptyProjection(RelNode input) {
        return LogicalProject.create(input, List.of(), List.of(), List.of());
    }

    private RelNode createIncludeProjection(RelNode input, String[] includes, RexBuilder rexBuilder)
            throws ConversionException {
        RelDataType rowType = input.getRowType();
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        for (String includePattern : includes) {
            if (includePattern.contains("*")) {
                resolveWildcardFields(includePattern, rowType, rexBuilder, projects, fieldNames);
            } else {
                resolveExactField(includePattern, rowType, rexBuilder, projects, fieldNames);
            }
        }

        return LogicalProject.create(input, List.of(), projects, fieldNames);
    }

    private void resolveWildcardFields(String pattern, RelDataType rowType, RexBuilder rexBuilder,
            List<RexNode> projects, List<String> fieldNames) {
        String regex = pattern.replace(".", "\\.").replace("*", ".*");
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (field.getName().matches(regex)) {
                projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
                fieldNames.add(field.getName());
            }
        }
    }

    private void resolveExactField(String fieldName, RelDataType rowType, RexBuilder rexBuilder,
            List<RexNode> projects, List<String> fieldNames) throws ConversionException {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        if (field == null) {
            throw ConversionException.invalidField(fieldName);
        }
        projects.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
        fieldNames.add(field.getName());
    }
}

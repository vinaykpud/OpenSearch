/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.pipeline.clause;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.opensearch.dsl.pipeline.AbstractClauseConverter;
import org.opensearch.dsl.pipeline.ConversionContext;
import org.opensearch.dsl.pipeline.PipelinePhase;
import org.opensearch.dsl.exception.ConversionException;

import java.util.List;

/**
 * Creates a LogicalTableScan from the pre-resolved index schema.
 *
 * Schema resolution (I/O, type mapping, caching) happens before the pipeline
 * runs — this converter only creates the Calcite plan node.
 */
public class IndexScanConverter extends AbstractClauseConverter {

    /** Creates an IndexScanConverter in the {@link PipelinePhase#SCHEMA} phase. */
    public IndexScanConverter() {
        super(PipelinePhase.SCHEMA);
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return true;
    }

    @Override
    protected void validate(ConversionContext ctx) throws ConversionException {
        ctx.requireRelNodeSupported(LogicalTableScan.class);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RelDataType schema = ctx.getIndexSchema();
        String indexName = ctx.getIndexName();

        Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return schema;
            }
        };

        RelOptTable relOptTable = RelOptTableImpl.create(
            null,
            schema,
            List.of(indexName),
            table,
            (org.apache.calcite.linq4j.tree.Expression) null
        );

        return LogicalTableScan.create(ctx.getCluster(), relOptTable, List.of());
    }
}

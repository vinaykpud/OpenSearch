/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} helper methods: extractResolvedBackend and extractTableName.
 */
public class PlanWalkerHelpersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    public void testExtractResolvedBackendFromTableScan() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        String backend = PlanWalker.extractResolvedBackend(scan);
        assertEquals("lucene", backend);
    }

    public void testExtractResolvedBackendFromNestedTree() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));

        // Inner wrapper (simulates project node)
        RelNode inner = mock(RelNode.class);
        when(inner.getInputs()).thenReturn(List.of(scan));

        // Outer wrapper (simulates filter node)
        RelNode outer = mock(RelNode.class);
        when(outer.getInputs()).thenReturn(List.of(inner));

        String backend = PlanWalker.extractResolvedBackend(outer);
        assertEquals("lucene", backend);
    }

    public void testExtractResolvedBackendReturnsBackendForStageInputScan() {
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("mock-parquet"));

        RelNode wrapper = mock(RelNode.class);
        when(wrapper.getInputs()).thenReturn(List.of(stageInput));

        String backend = PlanWalker.extractResolvedBackend(wrapper);
        assertEquals("mock-parquet", backend);
    }

    public void testExtractTableNameFromTableScan() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        String tableName = PlanWalker.extractTableName(scan);
        assertEquals("http_logs", tableName);
    }

    public void testExtractTableNameReturnsNullForStageInputScan() {
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("mock-parquet"));

        RelNode wrapper = mock(RelNode.class);
        when(wrapper.getInputs()).thenReturn(List.of(stageInput));

        String tableName = PlanWalker.extractTableName(wrapper);
        assertNull(tableName);
    }
}

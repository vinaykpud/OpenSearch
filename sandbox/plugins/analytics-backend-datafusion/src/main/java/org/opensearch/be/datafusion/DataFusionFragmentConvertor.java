/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage partial aggregate not yet implemented");
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        throw new UnsupportedOperationException("Multi-stage final aggregate not yet implemented");
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage fragment attachment not yet implemented");
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        ConverterProvider converterProvider = new ConverterProvider(extensions, typeFactory);
        return new SubstraitRelVisitor(converterProvider);
    }
}

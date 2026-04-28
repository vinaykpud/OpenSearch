/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Tests the scan, filter, aggregate, and project capability declarations on {@link DataFusionAnalyticsBackendPlugin}. */
public class DataFusionAnalyticsBackendPluginTests extends OpenSearchTestCase {

    private static final String FORMAT = "parquet";

    private static final Set<FieldType> EXPECTED_SUPPORTED_TYPES = buildSupportedTypes();

    private BackendCapabilityProvider provider;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        DataFusionPlugin plugin = new DataFusionPlugin() {
            @Override
            public String name() {
                return "datafusion";
            }

            @Override
            public List<String> getSupportedFormats() {
                return List.of(FORMAT);
            }
        };
        provider = new DataFusionAnalyticsBackendPlugin(plugin).getCapabilityProvider();
    }

    public void testProjectCapabilityDeclaresCast() {
        Set<ProjectCapability> caps = provider.projectCapabilities();
        assertEquals("expected exactly one project capability", 1, caps.size());

        ProjectCapability cap = caps.iterator().next();
        assertTrue("expected a Scalar capability", cap instanceof ProjectCapability.Scalar);
        ProjectCapability.Scalar scalar = (ProjectCapability.Scalar) cap;

        assertEquals(ScalarFunction.CAST, scalar.function());
        assertEquals(EXPECTED_SUPPORTED_TYPES, scalar.fieldTypes());
        assertEquals(Set.of(FORMAT), scalar.formats());
        assertTrue("literal evaluation must be declared", scalar.supportsLiteralEvaluation());
    }

    public void testScanCapabilityCoversAllSupportedFieldTypes() {
        Set<ScanCapability> scans = provider.scanCapabilities();
        assertEquals("expected exactly one scan capability", 1, scans.size());

        ScanCapability cap = scans.iterator().next();
        assertTrue("expected a DocValues scan capability", cap instanceof ScanCapability.DocValues);
        ScanCapability.DocValues docValues = (ScanCapability.DocValues) cap;

        assertEquals(Set.of(FORMAT), docValues.formats());
        assertEquals(EXPECTED_SUPPORTED_TYPES, docValues.supportedFieldTypes());
    }

    public void testFilterCapabilitiesCoverAllStandardOpsOnAllSupportedTypes() {
        Set<FilterOperator> expectedOps = Set.of(
            FilterOperator.EQUALS,
            FilterOperator.NOT_EQUALS,
            FilterOperator.GREATER_THAN,
            FilterOperator.GREATER_THAN_OR_EQUAL,
            FilterOperator.LESS_THAN,
            FilterOperator.LESS_THAN_OR_EQUAL,
            FilterOperator.IS_NULL,
            FilterOperator.IS_NOT_NULL,
            FilterOperator.IN,
            FilterOperator.LIKE
        );

        Set<FilterCapability> caps = provider.filterCapabilities();
        assertEquals(expectedOps.size() * EXPECTED_SUPPORTED_TYPES.size(), caps.size());

        for (FilterOperator op : expectedOps) {
            for (FieldType type : EXPECTED_SUPPORTED_TYPES) {
                assertTrue(
                    "missing filter capability for " + op + " on " + type,
                    caps.contains(new FilterCapability.Standard(op, Set.of(type), Set.of(FORMAT)))
                );
            }
        }
    }

    public void testAggregateCapabilitiesCoverAllDeclaredFunctions() {
        Set<AggregateFunction> expectedFns = Set.of(
            AggregateFunction.SUM,
            AggregateFunction.SUM0,
            AggregateFunction.MIN,
            AggregateFunction.MAX,
            AggregateFunction.COUNT,
            AggregateFunction.AVG
        );

        Set<AggregateCapability> caps = provider.aggregateCapabilities();
        assertEquals(expectedFns.size() * EXPECTED_SUPPORTED_TYPES.size(), caps.size());

        for (AggregateFunction fn : expectedFns) {
            for (FieldType type : EXPECTED_SUPPORTED_TYPES) {
                assertTrue(
                    "missing aggregate capability for " + fn + " on " + type,
                    caps.contains(AggregateCapability.simple(fn, Set.of(type), Set.of(FORMAT)))
                );
            }
        }
    }

    private static Set<FieldType> buildSupportedTypes() {
        Set<FieldType> types = EnumSet.noneOf(FieldType.class);
        types.addAll(FieldType.numeric());
        types.addAll(FieldType.keyword());
        types.addAll(FieldType.date());
        types.add(FieldType.BOOLEAN);
        return Set.copyOf(types);
    }
}

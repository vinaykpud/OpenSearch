/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.optimizer;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for QueryOptimizer.
 * 
 * Tests optimization rules including filter pushdown, projection pruning,
 * and error handling.
 */
public class QueryOptimizerTests extends OpenSearchTestCase {
    
    private QueryOptimizer optimizer;
    private RelBuilder relBuilder;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        optimizer = new CalciteQueryOptimizer();
        
        // Create a schema with a test table
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        SchemaPlus testSchema = rootSchema.add("test", new AbstractSchema());
        
        // Add a products table with some fields
        testSchema.add("products", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("category", typeFactory.createJavaType(String.class))
                    .add("price", typeFactory.createJavaType(Long.class))
                    .add("name", typeFactory.createJavaType(String.class))
                    .build();
            }
        });
        
        // Create RelBuilder with the schema
        relBuilder = RelBuilder.create(
            Frameworks.newConfigBuilder()
                .defaultSchema(testSchema)
                .build()
        );
    }
    
    /**
     * Test that filter pushdown works correctly.
     * 
     * Original plan:
     *   Project(a, b)
     *     Filter(a > 10)
     *       TableScan(test)
     * 
     * The optimizer may reorder or merge operations, but the semantics
     * should be preserved.
     */
    public void testFilterPushdownThroughProjection() throws Exception {
        // Build a plan: Project -> Filter -> TableScan
        RelNode plan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .project(
                relBuilder.field("category"),
                relBuilder.field("price")
            )
            .build();
        
        // Optimize the plan
        RelNode optimizedPlan = optimizer.optimize(plan);
        
        // Verify the plan structure
        assertNotNull("Optimized plan should not be null", optimizedPlan);
        
        // The optimized plan should contain a filter and a scan
        // The exact structure may vary based on optimization rules
        String planString = RelOptUtil.toString(optimizedPlan);
        assertTrue("Plan should contain Filter", planString.contains("Filter"));
        assertTrue("Plan should contain TableScan", planString.contains("TableScan"));
        
        logger.info("Original plan:\n{}", RelOptUtil.toString(plan));
        logger.info("Optimized plan:\n{}", RelOptUtil.toString(optimizedPlan));
    }
    
    /**
     * Test that projection pruning removes unused columns.
     * 
     * Original plan:
     *   Project(a)
     *     Project(a, b, c)
     *       TableScan(test)
     * 
     * Expected optimized plan:
     *   Project(a)
     *     TableScan(test)
     * 
     * (Inner project should be removed or merged)
     */
    public void testProjectionPruning() throws Exception {
        // Build a plan with redundant projections
        RelNode plan = relBuilder
            .scan("products")
            .project(
                relBuilder.field("category"),
                relBuilder.field("price"),
                relBuilder.field("name")
            )
            .project(
                relBuilder.field("category")
            )
            .build();
        
        // Optimize the plan
        RelNode optimizedPlan = optimizer.optimize(plan);
        
        // Verify the plan structure
        assertNotNull("Optimized plan should not be null", optimizedPlan);
        assertTrue("Root should be a Project", optimizedPlan instanceof LogicalProject);
        
        // The optimized plan should have merged or removed the redundant projection
        // Check that we don't have two consecutive projects
        RelNode child = optimizedPlan.getInput(0);
        
        // After optimization, the child should be TableScan (projects merged)
        // or at most one Project remains
        if (child instanceof LogicalProject) {
            // If there's still a project, its child should be TableScan
            RelNode grandchild = child.getInput(0);
            assertTrue("Grandchild should be TableScan", grandchild instanceof LogicalTableScan);
        } else {
            // Projects were fully merged, child is TableScan
            assertTrue("Child should be TableScan", child instanceof LogicalTableScan);
        }
        
        logger.info("Original plan:\n{}", RelOptUtil.toString(plan));
        logger.info("Optimized plan:\n{}", RelOptUtil.toString(optimizedPlan));
    }
    
    /**
     * Test that optimization handles errors gracefully.
     * 
     * When optimization fails, the original plan should be preserved
     * and a descriptive error should be returned.
     */
    public void testOptimizationErrorHandling() {
        // Test with null plan
        try {
            optimizer.optimize(null);
            fail("Should throw OptimizationException for null plan");
        } catch (OptimizationException e) {
            assertTrue("Error message should mention null", 
                e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("null"));
        }
        
        // Test with null config
        RelNode plan = relBuilder
            .scan("products")
            .build();
        
        try {
            optimizer.optimize(plan, null);
            fail("Should throw OptimizationException for null config");
        } catch (OptimizationException e) {
            assertTrue("Error message should mention null config", 
                e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("null"));
        }
    }
    
    /**
     * Test that default configuration works correctly.
     */
    public void testDefaultConfiguration() throws Exception {
        RelNode plan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .build();
        
        // Optimize with default config
        RelNode optimizedPlan = optimizer.optimize(plan);
        
        assertNotNull("Optimized plan should not be null", optimizedPlan);
        logger.info("Optimized with default config:\n{}", RelOptUtil.toString(optimizedPlan));
    }
    
    /**
     * Test that custom configuration works correctly.
     */
    public void testCustomConfiguration() throws Exception {
        RelNode plan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .build();
        
        // Create custom config with specific rules
        OptimizationConfig config = OptimizationConfig.builder()
            .withStandardRules()
            .withPlannerType(OptimizationConfig.PlannerType.HEP)
            .withMaxIterations(50)
            .build();
        
        // Optimize with custom config
        RelNode optimizedPlan = optimizer.optimize(plan, config);
        
        assertNotNull("Optimized plan should not be null", optimizedPlan);
        logger.info("Optimized with custom config:\n{}", RelOptUtil.toString(optimizedPlan));
    }
    
    /**
     * Test that VolcanoPlanner throws an exception (not implemented yet).
     * 
     * VolcanoPlanner is a cost-based optimizer that is not yet implemented.
     * Attempting to use it should throw an OptimizationException.
     */
    public void testVolcanoPlannerNotImplemented() {
        RelNode plan = relBuilder
            .scan("products")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("price"),
                    relBuilder.literal(100)
                )
            )
            .build();
        
        // Create config with VolcanoPlanner
        OptimizationConfig config = OptimizationConfig.builder()
            .withStandardRules()
            .withPlannerType(OptimizationConfig.PlannerType.VOLCANO)
            .build();
        
        // Should throw exception
        try {
            optimizer.optimize(plan, config);
            fail("Should throw OptimizationException for VolcanoPlanner");
        } catch (OptimizationException e) {
            assertTrue("Error message should mention VolcanoPlanner", 
                e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("volcano"));
            assertTrue("Error message should mention not implemented", 
                e.getMessage().toLowerCase(java.util.Locale.ROOT).contains("not implemented"));
        }
    }
}

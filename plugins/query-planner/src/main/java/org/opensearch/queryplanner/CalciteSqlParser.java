/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Collections;
import java.util.Properties;

/**
 * SQL Parser that uses Apache Calcite to parse SQL and produce RelNode.
 * This is the entry point of the optimizer pipeline.
 */
public class CalciteSqlParser {

    private final SchemaProvider schemaProvider;
    private final RelDataTypeFactory typeFactory;

    public CalciteSqlParser(SchemaProvider schemaProvider, RelDataTypeFactory typeFactory) {
        this.schemaProvider = schemaProvider;
        this.typeFactory = typeFactory;
    }

    /**
     * Parse SQL string and return a RelNode (logical plan).
     */
    public RelNode parse(String sql) throws SqlParseException {
        // 1. Parse SQL to AST - use JAVA lexer to keep identifiers lowercase
        SqlParser.Config parserConfig = SqlParser.config()
            .withLex(Lex.JAVA);

        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();

        // 2. Create catalog reader with schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        schemaProvider.registerTables(rootSchema);

        Properties props = new Properties();
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(""),
            typeFactory,
            config
        );

        // 3. Validate SQL
        SqlValidator validator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        );

        SqlNode validatedSql = validator.validate(sqlNode);

        // 4. Create planner and cluster
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        RelOptPlanner planner = new HepPlanner(programBuilder.build());

        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Set up metadata provider for explain() and other metadata operations
        // Use JaninoRelMetadataProvider with default chain for standard Calcite metadata
        org.apache.calcite.rel.metadata.RelMetadataProvider metadataProvider =
            org.apache.calcite.rel.metadata.JaninoRelMetadataProvider.of(
                org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE);
        cluster.setMetadataProvider(metadataProvider);
        cluster.setMetadataQuerySupplier(org.apache.calcite.rel.metadata.RelMetadataQuery::instance);

        // 5. Convert to RelNode
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true);

        SqlToRelConverter converter = new SqlToRelConverter(
            null,  // No view expander needed
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig
        );

        RelRoot relRoot = converter.convertQuery(validatedSql, false, true);
        return relRoot.rel;
    }
}

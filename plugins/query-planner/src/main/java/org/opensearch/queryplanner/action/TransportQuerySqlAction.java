/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.action;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.queryplanner.CalciteSqlParser;
import org.opensearch.queryplanner.coordinator.DefaultQueryCoordinator;
import org.opensearch.queryplanner.OpenSearchSchemaFactory;
import org.opensearch.queryplanner.coordinator.QueryCoordinator;
import org.opensearch.queryplanner.SchemaProvider;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action for executing SQL queries.
 *
 * <p>This action:
 * <ol>
 *   <li>Parses SQL to a logical plan using Calcite</li>
 *   <li>Passes the RelNode to QueryCoordinator for planning and execution</li>
 * </ol>
 */
public class TransportQuerySqlAction extends HandledTransportAction<QuerySqlRequest, QuerySqlResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQuerySqlAction.class);

    private final RelDataTypeFactory typeFactory;
    private final QueryCoordinator queryCoordinator;
    private final OpenSearchSchemaFactory schemaFactory;

    @Inject
    public TransportQuerySqlAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            IndicesService indicesService) {
        super(QuerySqlAction.NAME, transportService, actionFilters, QuerySqlRequest::new);
        this.typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.queryCoordinator = new DefaultQueryCoordinator(
            transportService, clusterService, indicesService, new RootAllocator());
        this.schemaFactory = new OpenSearchSchemaFactory(clusterService);
    }

    @Override
    protected void doExecute(Task task, QuerySqlRequest request, ActionListener<QuerySqlResponse> listener) {
        long startTime = System.currentTimeMillis();
        try {
            String sql = request.getSql();
            logger.info("Executing SQL: {}", sql);

            // Step 1: Build schema from cluster metadata
            SchemaProvider schemaProvider = schemaFactory.createSchemaProvider();

            // Step 2: Parse SQL to logical plan using Calcite
            CalciteSqlParser sqlParser = new CalciteSqlParser(schemaProvider, typeFactory);
            RelNode logicalPlan = sqlParser.parse(sql);
            logger.info("Parsed logical plan: {}", logicalPlan.explain());

            // Step 3: Execute via QueryCoordinator
            queryCoordinator.execute(logicalPlan)
                .whenComplete((root, error) -> {
                    long tookMillis = System.currentTimeMillis() - startTime;
                    if (error != null) {
                        logger.error("Error executing SQL: {}", sql, error);
                        listener.onFailure(error instanceof Exception ? (Exception) error : new RuntimeException(error));
                    } else {
                        try {
                            listener.onResponse(convertToResponse(root, tookMillis));
                        } finally {
                            root.close();
                        }
                    }
                });

        } catch (Exception e) {
            logger.error("Error executing SQL: {}", request.getSql(), e);
            listener.onFailure(e);
        }
    }

    /**
     * Convert VectorSchemaRoot to QuerySqlResponse.
     */
    private QuerySqlResponse convertToResponse(VectorSchemaRoot root, long tookMillis) {
        // Get column names
        List<String> columnNameList = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            columnNameList.add(vector.getName());
        }
        String[] columnNames = columnNameList.toArray(new String[0]);

        // Convert rows
        List<QuerySqlResponse.Row> rows = new ArrayList<>();
        int rowCount = root.getRowCount();
        int columnCount = root.getFieldVectors().size();

        for (int row = 0; row < rowCount; row++) {
            Object[] rowData = new Object[columnCount];
            for (int col = 0; col < columnCount; col++) {
                FieldVector vector = root.getFieldVectors().get(col);
                rowData[col] = vector.getObject(row);
            }
            rows.add(new QuerySqlResponse.Row(rowData));
        }

        return new QuerySqlResponse(rows, columnNames, tookMillis);
    }

}

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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.queryplanner.physical.exec.ExecNode;
import org.opensearch.queryplanner.physical.operator.LuceneDataProvider;
import org.opensearch.queryplanner.physical.operator.Operator;
import org.opensearch.queryplanner.physical.operator.OperatorFactory;
import org.opensearch.queryplanner.physical.operator.ScanOperator;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action that executes a query plan fragment on a shard.
 */
public class TransportShardQueryPlanAction extends TransportAction<ShardQueryPlanRequest, ShardQueryPlanResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardQueryPlanAction.class);
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private final IndicesService indicesService;
    private final BufferAllocator allocator;

    @Inject
    public TransportShardQueryPlanAction(
            TransportService transportService,
            ActionFilters actionFilters,
            IndicesService indicesService) {
        super(ShardQueryPlanAction.NAME, actionFilters, transportService.getTaskManager());
        this.indicesService = indicesService;
        this.allocator = new RootAllocator();

        transportService.registerRequestHandler(
            ShardQueryPlanAction.NAME,
            ThreadPool.Names.SEARCH,
            ShardQueryPlanRequest::new,
            this::handleShardRequest
        );
    }

    @Override
    protected void doExecute(Task task, ShardQueryPlanRequest request, ActionListener<ShardQueryPlanResponse> listener) {
        try {
            listener.onResponse(executeOnShard(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void handleShardRequest(ShardQueryPlanRequest request, TransportChannel channel, Task task) {
        try {
            channel.sendResponse(executeOnShard(request));
        } catch (Exception e) {
            logger.error("Error handling shard request", e);
            try {
                channel.sendResponse(e);
            } catch (Exception ex) {
                logger.error("Failed to send error response", ex);
            }
        }
    }

    private ShardQueryPlanResponse executeOnShard(ShardQueryPlanRequest request) {
        ShardId shardId = request.shardId();

        try {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            IndexShard shard = indexService.getShard(shardId.id());
            MapperService mapperService = indexService.mapperService();

            try (Engine.Searcher searcher = shard.acquireSearcher("query-plan-shard")) {
                LuceneDataProvider dataProvider = new LuceneDataProvider(
                    searcher, mapperService, DEFAULT_BATCH_SIZE);

                VectorSchemaRoot result = executeFragment(request.getPlanFragment(), dataProvider);
                ShardQueryPlanResponse response = convertToResponse(shardId, result);
                result.close();
                return response;
            }
        } catch (Exception e) {
            logger.error("Error executing on shard {}", shardId, e);
            return new ShardQueryPlanResponse(shardId, e.getMessage());
        }
    }

    private VectorSchemaRoot executeFragment(ExecNode planFragment, ScanOperator.DataProvider dataProvider) {
        OperatorFactory factory = new OperatorFactory(dataProvider);
        Operator rootOp = factory.create(planFragment);

        rootOp.open(allocator);
        try {
            List<VectorSchemaRoot> batches = new ArrayList<>();
            VectorSchemaRoot batch;
            while ((batch = rootOp.next()) != null) {
                batches.add(batch);
            }

            if (batches.isEmpty()) {
                return VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                    allocator
                );
            }

            if (batches.size() == 1) {
                return batches.get(0);
            }

            for (int i = 1; i < batches.size(); i++) {
                batches.get(i).close();
            }
            return batches.get(0);
        } finally {
            rootOp.close();
        }
    }

    private ShardQueryPlanResponse convertToResponse(ShardId shardId, VectorSchemaRoot result) {
        List<String> columnNames = new ArrayList<>();
        for (var field : result.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        List<Object[]> rows = new ArrayList<>();
        for (int row = 0; row < result.getRowCount(); row++) {
            Object[] rowData = new Object[columnNames.size()];
            for (int col = 0; col < columnNames.size(); col++) {
                rowData[col] = getValue(result.getVector(col), row);
            }
            rows.add(rowData);
        }

        return new ShardQueryPlanResponse(shardId, columnNames, rows);
    }

    private Object getValue(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(row);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(row);
        }
        return null;
    }
}

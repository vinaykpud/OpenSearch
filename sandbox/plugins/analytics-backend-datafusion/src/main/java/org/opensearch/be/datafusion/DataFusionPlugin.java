/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Owns the {@link DataFusionService} lifecycle (memory pool, native runtime).
 * Analytics query capabilities are declared in {@link DataFusionAnalyticsExtension},
 * which is SPI-discovered and receives this plugin instance via its constructor.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin<DatafusionReader>, DataFormatPlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    /** Memory pool limit for the DataFusion runtime. */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        Runtime.getRuntime().maxMemory() / 4,
        0L,
        Setting.Property.NodeScope
    );

    /** Spill memory limit — when exceeded, DataFusion spills to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        Runtime.getRuntime().maxMemory() / 8,
        0L,
        Setting.Property.NodeScope
    );

    private static final Set<FieldTypeCapabilities.Capability> COLUMNAR_POINT_STORED = Set.of(
        FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
        FieldTypeCapabilities.Capability.POINT_RANGE,
        FieldTypeCapabilities.Capability.STORED_FIELDS
    );

    private static final Set<FieldTypeCapabilities.Capability> COLUMNAR_FULLTEXT_STORED = Set.of(
        FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
        FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH,
        FieldTypeCapabilities.Capability.STORED_FIELDS
    );

    static final DataFormat PARQUET_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of(
                new FieldTypeCapabilities("keyword", COLUMNAR_FULLTEXT_STORED),
                new FieldTypeCapabilities("short", COLUMNAR_POINT_STORED),
                new FieldTypeCapabilities("integer", COLUMNAR_POINT_STORED),
                new FieldTypeCapabilities("long", COLUMNAR_POINT_STORED),
                new FieldTypeCapabilities("float", COLUMNAR_POINT_STORED),
                new FieldTypeCapabilities("double", COLUMNAR_POINT_STORED),
                new FieldTypeCapabilities("date", COLUMNAR_POINT_STORED)
            );
        }
    };

    private volatile DataFusionService dataFusionService;

    public DataFusionPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();
        long memoryPoolLimit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long spillMemoryLimit = DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        dataFusionService = DataFusionService.builder()
            .memoryPoolLimit(memoryPoolLimit)
            .spillMemoryLimit(spillMemoryLimit)
            .spillDirectory(spillDir)
            .build();
        dataFusionService.start();
        logger.info("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        return Collections.singletonList(dataFusionService);
    }

    /** Package-private so {@link DataFusionAnalyticsExtension} can access it. */
    DataFusionService getDataFusionService() {
        return dataFusionService;
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new DatafusionReaderManager(format, shardPath, dataFusionService);
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(PARQUET_FORMAT);
    }

    @Override
    public DataFormat getDataFormat() {
        return PARQUET_FORMAT;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings) {
        // DataFusion is a read-only query engine; indexing is not supported.
        return null;
    }

    @Override
    public void close() throws IOException {
        if (dataFusionService != null) {
            dataFusionService.close();
        }
    }
}

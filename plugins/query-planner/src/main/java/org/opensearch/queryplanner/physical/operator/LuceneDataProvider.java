/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.physical.operator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.List;

/**
 * DataProvider implementation that reads from Lucene doc values.
 *
 * <p>This is the bridge between the Volcano-style Operator execution model
 * and Lucene's index structures. It:
 * <ul>
 *   <li>Executes a Lucene query to find matching documents</li>
 *   <li>Uses DocValuesCollector to read into arrow batches</li>
 *   <li>Returns Arrow VectorSchemaRoot batches</li>
 * </ul>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * LuceneDataProvider provider = new LuceneDataProvider(
 *     indexSearcher,
 *     query,
 *     mapperService,
 *     1024  // batch size
 * );
 *
 * ScanOperator scan = new ScanOperator(indexName, columns, provider);
 * }</pre>
 */
public class LuceneDataProvider implements ScanOperator.DataProvider {

    private final IndexSearcher searcher;
    private final MapperService mapperService;
    private final int batchSize;

    // Query can be set at construction or via open() with filter expression
    private Query query;
    private DocValuesCollector collector;
    private boolean executed = false;
    private boolean exhausted = false;

    /**
     * Create a LuceneDataProvider with an explicit Lucene query.
     *
     * @param searcher The IndexSearcher to use
     * @param query The Lucene query (use MatchAllDocsQuery for no filter)
     * @param mapperService MapperService for field type info
     * @param batchSize Number of documents per batch
     */
    public LuceneDataProvider(IndexSearcher searcher, Query query,
                               MapperService mapperService, int batchSize) {
        this.searcher = searcher;
        this.query = query;
        this.mapperService = mapperService;
        this.batchSize = batchSize;
    }

    /**
     * Create a LuceneDataProvider without a pre-set query.
     * The query will be built from the filter expression in open().
     *
     * @param searcher The IndexSearcher to use
     * @param mapperService MapperService for field type info
     * @param batchSize Number of documents per batch
     */
    public LuceneDataProvider(IndexSearcher searcher, MapperService mapperService, int batchSize) {
        this(searcher, null, mapperService, batchSize);
    }

    @Override
    public void open(String indexName, List<String> columns, String filter) {
        this.collector = new DocValuesCollector(columns, mapperService, batchSize);
        this.executed = false;
        this.exhausted = false;

        // For doc value scans, we scan all docs and let the execution engine handle filtering
        // TODO: Implement filter pushdown / construct query higher up and pass down
        if (this.query == null) {
            this.query = new MatchAllDocsQuery();
        }
    }

    @Override
    public VectorSchemaRoot nextBatch(BufferAllocator allocator) {
        if (exhausted) {
            return null;
        }

        if (!executed) {
            try {
                searcher.search(query, collector);
                executed = true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to execute Lucene query", e);
            }
        }

        // Convert collected docs to Arrow
        VectorSchemaRoot batch = collector.toVectorSchemaRoot(allocator);
        exhausted = true;  // For now, single batch

        return batch;
    }

    @Override
    public void close() {
        // Collector cleanup if needed
        if (collector != null) {
            collector.reset();
        }
    }

    /**
     * Get the query being executed.
     */
    public Query getQuery() {
        return query;
    }
}

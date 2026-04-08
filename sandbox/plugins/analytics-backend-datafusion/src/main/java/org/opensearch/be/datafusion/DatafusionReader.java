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
import org.opensearch.be.datafusion.jni.ReaderHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

/**
 * DataFusion reader for JNI operations.
 * <p>
 * Each reader represents a point-in-time snapshot of parquet/arrow files for a shard.
 * Created from a catalog snapshot during refresh; closed when associated catalog snapshot is removed
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);
    private final String directoryPath;
    private final ReaderHandle readerHandle;

    /**
     * Creates a DatafusionReader for the given shard directory and files.
     *
     * @param directoryPath shard data directory
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> files) {
        this.directoryPath = directoryPath;
        String[] fileNames = new String[0];
        if (files != null) {
            fileNames = files.stream().flatMap(writerFileSet -> writerFileSet.files().stream()).toArray(String[]::new);
        }
        readerHandle = new ReaderHandle(directoryPath, fileNames);
    }

    /**
     * Creates a mock DatafusionReader backed by the bundled parquet resource.
     * Extracts the resource to a temp directory on first call and reuses it.
     */
    static DatafusionReader createMock() {
        Path dir = extractMockParquet();
        logger.info("Creating mock DatafusionReader from resource at [{}]", dir);
        return new DatafusionReader(dir.toString(), new String[] { MOCK_RESOURCE });
    }

    private DatafusionReader(String directoryPath, String[] fileNames) {
        this.directoryPath = directoryPath;
        this.readerHandle = new ReaderHandle(directoryPath, fileNames);
    }

    private static Path mockDir;
    private static final String MOCK_RESOURCE = "mock_data.parquet";

    private static synchronized Path extractMockParquet() {
        if (mockDir != null && Files.exists(mockDir.resolve(MOCK_RESOURCE))) {
            return mockDir;
        }
        try {
            Path tmpDir = Files.createTempDirectory("datafusion-mock");
            try (InputStream in = DatafusionReader.class.getClassLoader().getResourceAsStream(MOCK_RESOURCE)) {
                if (in == null) {
                    throw new IOException("Mock resource not found on classpath: " + MOCK_RESOURCE);
                }
                Files.copy(in, tmpDir.resolve(MOCK_RESOURCE), StandardCopyOption.REPLACE_EXISTING);
            }
            mockDir = tmpDir;
            return mockDir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract mock parquet resource", e);
        }
    }

    /**
     * Wraps a pre-existing native reader pointer (test only).
     * The caller retains ownership — this reader will NOT close the handle.
     */
    DatafusionReader(long nativePtr) {
        this.directoryPath = "";
        this.readerHandle = ReaderHandle.wrap(nativePtr);
    }

    @Override
    public void close() throws IOException {
        readerHandle.close();
        logger.debug("DatafusionReader closed for [{}]", directoryPath);
    }

    /**
     * Returns the type-safe handle to the native reader.
     * Callers should hold this reference and call
     * {@link ReaderHandle#getPointer()} only at JNI invocation time.
     */
    public ReaderHandle getReaderHandle() {
        return readerHandle;
    }
}

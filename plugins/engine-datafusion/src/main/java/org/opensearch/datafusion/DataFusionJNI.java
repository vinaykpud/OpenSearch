/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * JNI wrapper for DataFusion operations
 */
public class DataFusionJNI {

    /**
     * Default constructor for DataFusionJNI.
     */
    public DataFusionJNI() {
        // Default constructor
    }

    private static boolean libraryLoaded = false;

    static {
        loadNativeLibrary();
    }

    /**
     * Load the native library from resources
     */
    private static synchronized void loadNativeLibrary() {
        if (libraryLoaded) {
            return;
        }

        try {
            String osName = System.getProperty("os.name").toLowerCase();
            String libExtension;
            String libName;

            if (osName.contains("windows")) {
                libExtension = ".dll";
                libName = "libopensearch_datafusion_jni.dll";
            } else if (osName.contains("mac")) {
                libExtension = ".dylib";
                libName = "libopensearch_datafusion_jni.dylib";
            } else {
                libExtension = ".so";
                libName = "libopensearch_datafusion_jni.so";
            }

            // Try to load from resources first
            InputStream libStream = DataFusionJNI.class.getResourceAsStream("/native/" + libName);
            if (libStream != null) {
                // Extract to temporary file and load
                Path tempLib = Files.createTempFile("libopensearch_datafusion_jni", libExtension);
                Files.copy(libStream, tempLib, StandardCopyOption.REPLACE_EXISTING);
                tempLib.toFile().deleteOnExit();
                System.load(tempLib.toAbsolutePath().toString());
                libStream.close();
            } else {
                // Fallback to system library path
                System.loadLibrary("opensearch_datafusion_jni");
            }

            libraryLoaded = true;
        } catch (IOException | UnsatisfiedLinkError e) {
            throw new RuntimeException("Failed to load DataFusion JNI library", e);
        }
    }

    /**
     * Get version information
     * @return JSON string with version information
     */
    public static native String getVersion();

    /**
     * Create a new DataFusion context and register tables
     * @param parquetFilePath Path to the parquet file to register as a table
     * @return context ID
     */
    public static native long nativeCreateContext(String parquetFilePath);

    /**
     * Close a DataFusion context
     * @param contextId the context ID to close
     */
    public static native void nativeCloseContext(long contextId);

    /**
     * Execute a Substrait query plan
     * @param contextId the DataFusion context ID
     * @param queryPlanIR the Substrait query plan as bytes
     * @return JSON result from the query execution
     */
    public static native String nativeExecuteSubstraitQueryPlan(long contextId, byte[] queryPlanIR);
}

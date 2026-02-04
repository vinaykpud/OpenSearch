/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.queryplanner.engine.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * JNI bridge to DataFusion native library.
 *
 * <p>This class provides native method declarations that map to the Rust JNI
 * implementation in {@code jni/src/lib.rs}. All DataFrame operations are lazy
 * until {@link #collect(long)} is called.
 *
 * <h2>Usage Pattern:</h2>
 * <pre>{@code
 * long ctx = DataFusionBridge.createContext();
 * try {
 *     long df = DataFusionBridge.scan(ctx, "orders");
 *     df = DataFusionBridge.filter(df, "amount > 100");
 *     df = DataFusionBridge.project(df, new String[]{"category", "amount"});
 *     byte[] result = DataFusionBridge.collect(df);
 *     DataFusionBridge.freeDataFrame(df);
 *     // Deserialize Arrow IPC bytes...
 * } finally {
 *     DataFusionBridge.freeContext(ctx);
 * }
 * }</pre>
 *
 * <h2>Memory Management:</h2>
 * <ul>
 *   <li>Context handles must be freed with {@link #freeContext(long)}</li>
 *   <li>DataFrame handles must be freed with {@link #freeDataFrame(long)}</li>
 *   <li>Each operation returns a NEW DataFrame handle (old one is still valid)</li>
 * </ul>
 */
public class DataFusionBridge {

    private static final Logger logger = LogManager.getLogger(DataFusionBridge.class);

    private static volatile boolean libraryLoaded = false;
    private static volatile String loadError = null;

    private static final String LIBRARY_NAME = "opensearch_query_planner_jni";

    /**
     * Load the native library. Called automatically when first native method is used.
     *
     * <p>Tries multiple loading strategies:
     * <ol>
     *   <li>System.load() with absolute paths from known locations (preferred)</li>
     *   <li>System.loadLibrary() - uses java.library.path (fallback)</li>
     * </ol>
     */
    public static synchronized void loadLibrary() {
        if (libraryLoaded) {
            return;
        }

        // Strategy 1: Try known paths with System.load (absolute path)
        // This is preferred because it ensures we load the correct version
        List<String> searchPaths = getLibrarySearchPaths();
        String libFileName = getLibraryFileName();

        for (String searchPath : searchPaths) {
            Path libPath = Paths.get(searchPath, libFileName);
            File libFile = libPath.toFile();
            if (libFile.exists() && libFile.isFile()) {
                try {
                    System.load(libFile.getAbsolutePath());
                    libraryLoaded = true;
                    logger.info("Loaded DataFusion native library from: {}", libFile.getAbsolutePath());
                    return;
                } catch (UnsatisfiedLinkError e) {
                    logger.debug("System.load failed for {}: {}", libFile.getAbsolutePath(), e.getMessage());
                }
            }
        }

        // Strategy 2: Try System.loadLibrary (uses java.library.path)
        try {
            System.loadLibrary(LIBRARY_NAME);
            libraryLoaded = true;
            logger.info("Loaded DataFusion native library via System.loadLibrary");
            return;
        } catch (UnsatisfiedLinkError e) {
            logger.debug("System.loadLibrary failed: {}", e.getMessage());
        }

        // All strategies failed
        loadError = "Could not find " + libFileName + " in known locations: " + searchPaths;
        logger.error("Failed to load DataFusion native library: {}", loadError);
        throw new UnsatisfiedLinkError(loadError);
    }

    /**
     * Get the platform-specific library filename.
     */
    private static String getLibraryFileName() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("mac") || os.contains("darwin")) {
            return "lib" + LIBRARY_NAME + ".dylib";
        } else if (os.contains("win")) {
            return LIBRARY_NAME + ".dll";
        } else {
            return "lib" + LIBRARY_NAME + ".so";
        }
    }

    /**
     * Get list of paths to search for the native library.
     */
    private static List<String> getLibrarySearchPaths() {
        List<String> paths = new ArrayList<>();

        // Current working directory relative paths
        String cwd = System.getProperty("user.dir", ".");
        paths.add(Paths.get(cwd, "plugins", "query-planner", "jni", "target", "release").toString());
        paths.add(Paths.get(cwd, "jni", "target", "release").toString());

        // Try to find from class location (works during tests and production)
        try {
            java.net.URL classUrl = DataFusionBridge.class.getProtectionDomain().getCodeSource().getLocation();
            if (classUrl != null) {
                Path classPath = Paths.get(classUrl.toURI());
                // Walk up to find plugin root and then jni/target/release
                Path pluginRoot = classPath;
                for (int i = 0; i < 10 && pluginRoot != null; i++) {
                    Path jniPath = pluginRoot.resolve("jni/target/release");
                    if (jniPath.toFile().exists()) {
                        paths.add(jniPath.toString());
                        break;
                    }
                    pluginRoot = pluginRoot.getParent();
                }
            }
        } catch (Exception e) {
            logger.debug("Could not determine class location: {}", e.getMessage());
        }

        // Add java.library.path entries
        String libPath = System.getProperty("java.library.path", "");
        for (String path : libPath.split(File.pathSeparator)) {
            if (!path.isEmpty()) {
                paths.add(path);
            }
        }

        return paths;
    }

    /**
     * Check if the native library is available.
     */
    public static boolean isAvailable() {
        if (libraryLoaded) {
            return true;
        }
        try {
            loadLibrary();
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    /**
     * Get the error message if library loading failed.
     */
    public static String getLoadError() {
        return loadError;
    }

    // =========================================================================
    // Context Management
    // =========================================================================

    /**
     * Create a new DataFusion SessionContext.
     *
     * @return Handle (pointer) to the context
     */
    public static native long createContext();

    /**
     * Free a SessionContext.
     *
     * @param ctxPtr Handle to the context
     */
    public static native void freeContext(long ctxPtr);

    // =========================================================================
    // Data Sources
    // =========================================================================

    /**
     * Scan a table by name and return a lazy DataFrame handle.
     *
     * <p>The native engine determines how to read the data based on the table name.
     * Currently uses hardcoded test data; future implementation will connect to
     * actual data sources.
     *
     * @param ctxPtr Handle to the context
     * @param tableName Name of the table to scan
     * @return Handle to the DataFrame
     * @throws RuntimeException if scan fails
     */
    public static native long scan(long ctxPtr, String tableName);

    // =========================================================================
    // DataFrame Operations (all lazy - return new DataFrame handle)
    // =========================================================================

    /**
     * Apply a filter to the DataFrame.
     *
     * <p>The filter expression is a SQL-style expression string (e.g., "price > 100").
     *
     * @param dfPtr Handle to the DataFrame
     * @param filterExpr SQL filter expression
     * @return Handle to the new filtered DataFrame
     * @throws RuntimeException if filter fails
     */
    public static native long filter(long dfPtr, String filterExpr);

    /**
     * Project (select) columns from the DataFrame.
     *
     * @param dfPtr Handle to the DataFrame
     * @param columns Column names to select
     * @return Handle to the new projected DataFrame
     * @throws RuntimeException if projection fails
     */
    public static native long project(long dfPtr, String[] columns);

    /**
     * Aggregate the DataFrame with GROUP BY.
     *
     * @param dfPtr Handle to the DataFrame
     * @param groupBy Column names to group by (can be empty for global aggregation)
     * @param aggFuncs Aggregate function names (SUM, COUNT, AVG, MIN, MAX)
     * @param aggCols Column names to aggregate ("*" for COUNT(*))
     * @return Handle to the new aggregated DataFrame
     * @throws RuntimeException if aggregation fails
     */
    public static native long aggregate(long dfPtr, String[] groupBy, String[] aggFuncs, String[] aggCols);

    /**
     * Sort the DataFrame.
     *
     * @param dfPtr Handle to the DataFrame
     * @param columns Column names to sort by
     * @param ascending Whether each column is ascending (true) or descending (false)
     * @return Handle to the new sorted DataFrame
     * @throws RuntimeException if sorting fails
     */
    public static native long sort(long dfPtr, String[] columns, boolean[] ascending);

    /**
     * Limit the number of rows in the DataFrame.
     *
     * @param dfPtr Handle to the DataFrame
     * @param count Maximum number of rows
     * @return Handle to the new limited DataFrame
     * @throws RuntimeException if limit fails
     */
    public static native long limit(long dfPtr, int count);

    // =========================================================================
    // Execution
    // =========================================================================

    /**
     * Execute the DataFrame plan and collect results as Arrow IPC bytes.
     *
     * <p>This is where DataFusion optimizes the entire plan and executes it.
     * All previous operations (filter, project, etc.) are lazy until this is called.
     *
     * @param dfPtr Handle to the DataFrame
     * @return Arrow IPC serialized result bytes
     * @throws RuntimeException if execution fails
     */
    public static native byte[] collect(long dfPtr);

    /**
     * Free a DataFrame handle.
     *
     * @param dfPtr Handle to the DataFrame
     */
    public static native void freeDataFrame(long dfPtr);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.planner.coordinator;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics collected during query execution.
 *
 * <p>Tracks:
 * <ul>
 *   <li>Total execution time</li>
 *   <li>Per-segment execution time</li>
 *   <li>Documents processed</li>
 *   <li>Rows produced</li>
 *   <li>Segments executed</li>
 * </ul>
 *
 * <p>These statistics help with:
 * <ul>
 *   <li>Performance monitoring</li>
 *   <li>Query optimization</li>
 *   <li>Debugging slow queries</li>
 * </ul>
 */
public class ExecutionStatistics {

    private final long totalTimeMillis;
    private final int segmentsExecuted;
    private final long documentsProcessed;
    private final long rowsProduced;
    private final Map<String, Long> segmentTimes;

    /**
     * Constructs a new ExecutionStatistics.
     *
     * @param totalTimeMillis total execution time in milliseconds
     * @param segmentsExecuted number of segments executed
     * @param documentsProcessed number of documents processed
     * @param rowsProduced number of rows produced
     * @param segmentTimes per-segment execution times
     */
    public ExecutionStatistics(
        long totalTimeMillis,
        int segmentsExecuted,
        long documentsProcessed,
        long rowsProduced,
        Map<String, Long> segmentTimes
    ) {
        this.totalTimeMillis = totalTimeMillis;
        this.segmentsExecuted = segmentsExecuted;
        this.documentsProcessed = documentsProcessed;
        this.rowsProduced = rowsProduced;
        this.segmentTimes = segmentTimes != null ? new HashMap<>(segmentTimes) : new HashMap<>();
    }

    /**
     * Gets the total execution time in milliseconds.
     *
     * @return total time in milliseconds
     */
    public long getTotalTimeMillis() {
        return totalTimeMillis;
    }

    /**
     * Gets the number of segments executed.
     *
     * @return number of segments
     */
    public int getSegmentsExecuted() {
        return segmentsExecuted;
    }

    /**
     * Gets the number of documents processed.
     *
     * @return number of documents
     */
    public long getDocumentsProcessed() {
        return documentsProcessed;
    }

    /**
     * Gets the number of rows produced.
     *
     * @return number of rows
     */
    public long getRowsProduced() {
        return rowsProduced;
    }

    /**
     * Gets per-segment execution times.
     *
     * @return map of segment ID to execution time in milliseconds
     */
    public Map<String, Long> getSegmentTimes() {
        return Map.copyOf(segmentTimes);
    }

    /**
     * Creates a new builder for ExecutionStatistics.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ExecutionStatistics.
     */
    public static class Builder {
        private long totalTimeMillis;
        private int segmentsExecuted;
        private long documentsProcessed;
        private long rowsProduced;
        private Map<String, Long> segmentTimes = new HashMap<>();

        public Builder withTotalTimeMillis(long totalTimeMillis) {
            this.totalTimeMillis = totalTimeMillis;
            return this;
        }

        public Builder withSegmentsExecuted(int segmentsExecuted) {
            this.segmentsExecuted = segmentsExecuted;
            return this;
        }

        public Builder withDocumentsProcessed(long documentsProcessed) {
            this.documentsProcessed = documentsProcessed;
            return this;
        }

        public Builder withRowsProduced(long rowsProduced) {
            this.rowsProduced = rowsProduced;
            return this;
        }

        public Builder withSegmentTime(String segmentId, long timeMillis) {
            this.segmentTimes.put(segmentId, timeMillis);
            return this;
        }

        public ExecutionStatistics build() {
            return new ExecutionStatistics(
                totalTimeMillis,
                segmentsExecuted,
                documentsProcessed,
                rowsProduced,
                segmentTimes
            );
        }
    }

    @Override
    public String toString() {
        return String.format(
            "ExecutionStatistics[totalTime=%dms, segments=%d, docs=%d, rows=%d]",
            totalTimeMillis,
            segmentsExecuted,
            documentsProcessed,
            rowsProduced
        );
    }
}

package com.netflix.concurrency.limits;

/**
 * Common metric ids
 */
public final class MetricIds {
    public static final String LIMIT_GUAGE_NAME = "limit";
    public static final String INFLIGHT_GUAGE_NAME = "inflight";
    public static final String PARTITION_LIMIT_GUAGE_NAME = "limit.partition";
    public static final String MIN_RTT_NAME = "min_rtt";
    public static final String WINDOW_MIN_RTT_NAME = "min_window_rtt";
    public static final String WINDOW_QUEUE_SIZE_NAME = "queue_size";

    private MetricIds() {}
}

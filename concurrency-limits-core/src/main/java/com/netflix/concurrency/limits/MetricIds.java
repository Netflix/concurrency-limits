/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits;

/**
 * Common metric ids
 */
public final class MetricIds {
    public static final String LIMIT_NAME = "limit";
    public static final String CALL_NAME = "call";
    public static final String INFLIGHT_NAME = "inflight";
    public static final String PARTITION_LIMIT_NAME = "limit.partition";
    public static final String MIN_RTT_NAME = "min_rtt";
    public static final String WINDOW_MIN_RTT_NAME = "min_window_rtt";
    public static final String WINDOW_QUEUE_SIZE_NAME = "queue_size";

    private MetricIds() {}
}

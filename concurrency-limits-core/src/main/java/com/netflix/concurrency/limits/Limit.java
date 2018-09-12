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

import java.util.function.Consumer;

/**
 * Contract for an algorithm that calculates a concurrency limit based on 
 * rtt measurements
 */
public interface Limit {
    /**
     * @return Current estimated limit
     */
    int getLimit();

    /**
     * Register a callback to receive notification whenever the limit is updated to a new value
     * @param consumer
     */
    void notifyOnChange(Consumer<Integer> consumer);

    /**
     * Update the limiter with a sample
     * @param startTime
     * @param rtt
     * @param inflight
     * @param didDrop
     */
    void onSample(long startTime, long rtt, int inflight, boolean didDrop);
}

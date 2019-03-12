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
package com.netflix.concurrency.limits.limit.window;

import java.util.concurrent.TimeUnit;

class ImmutableAverageSampleWindow implements SampleWindow {
    private final long minRtt;
    private final long sum;
    private final int maxInFlight;
    private final int sampleCount;
    private final boolean didDrop;
    
    ImmutableAverageSampleWindow() {
        this.minRtt = Long.MAX_VALUE;
        this.sum = 0;
        this.maxInFlight = 0;
        this.sampleCount = 0;
        this.didDrop = false;
    }
    
    ImmutableAverageSampleWindow(long minRtt, long sum, int maxInFlight, int sampleCount, boolean didDrop) {
        this.minRtt = minRtt;
        this.sum = sum;
        this.maxInFlight = maxInFlight;
        this.sampleCount = sampleCount;
        this.didDrop = didDrop;
    }

    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getTrackedRttNanos() {
        return sampleCount == 0 ? 0 : sum / sampleCount;
    }

    @Override
    public int getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public int getSampleCount() {
        return sampleCount;
    }

    @Override
    public boolean didDrop() {
        return didDrop;
    }

    long getSum() {
        return sum;
    }

    @Override
    public String toString() {
        return "ImmutableAverageSampleWindow ["
                + "minRtt=" + TimeUnit.NANOSECONDS.toMicros(minRtt) / 1000.0
                + ", avgRtt=" + TimeUnit.NANOSECONDS.toMicros(getTrackedRttNanos()) / 1000.0
                + ", maxInFlight=" + maxInFlight
                + ", sampleCount=" + sampleCount
                + ", didDrop=" + didDrop + "]";
    }
}

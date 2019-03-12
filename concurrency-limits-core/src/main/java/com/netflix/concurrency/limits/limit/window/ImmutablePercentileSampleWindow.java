/**
 * Copyright 2019 Netflix, Inc.
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

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

class ImmutablePercentileSampleWindow implements SampleWindow {
    private final long minRtt;
    private final int maxInFlight;
    private final boolean didDrop;
    private final PriorityQueue<Long> sortedRtts;
    private final double percentile;

    ImmutablePercentileSampleWindow(double percentile) {
        this.minRtt = Long.MAX_VALUE;
        this.maxInFlight = 0;
        this.didDrop = false;
        this.sortedRtts = new PriorityQueue<>();
        this.percentile = percentile;
    }

    ImmutablePercentileSampleWindow(
            long minRtt,
            int maxInFlight,
            boolean didDrop,
            PriorityQueue<Long> sortedRtts,
            double percentile
    ) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.didDrop = didDrop;
        this.sortedRtts = sortedRtts;
        this.percentile = percentile;
    }

    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getTrackedRttNanos() {
        int rttIndex = (int) Math.round(sortedRtts.size() * percentile);
        int zeroBasedRttIndex = rttIndex - 1;
        return sortedRtts.toArray(new Long[]{})[zeroBasedRttIndex];
    }

    @Override
    public int getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public int getSampleCount() {
        return sortedRtts.size();
    }

    @Override
    public boolean didDrop() {
        return didDrop;
    }

    PriorityQueue<Long> getSortedRtts() {
        return sortedRtts;
    }

    @Override
    public String toString() {
        return "ImmutablePercentileSampleWindow ["
                + "minRtt=" + TimeUnit.NANOSECONDS.toMicros(minRtt) / 1000.0
                + ", p" + percentile + " rtt=" + TimeUnit.NANOSECONDS.toMicros(getTrackedRttNanos()) / 1000.0
                + ", maxInFlight=" + maxInFlight
                + ", sampleCount=" + sortedRtts.size()
                + ", didDrop=" + didDrop + "]";
    }
}

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

public class ImmutablePercentileSampleWindow implements SampleWindow<ImmutablePercentileSampleWindow> {
    private final long minRtt;
    private final int maxInFlight;
    private final boolean didDrop;
    private final PriorityQueue<Long> sortedRtts;
    private final int percentile;

    public ImmutablePercentileSampleWindow(int percentile) {
        this.minRtt = Long.MAX_VALUE;
        this.maxInFlight = 0;
        this.didDrop = false;
        this.sortedRtts = new PriorityQueue<>();
        this.percentile = percentile;
    }

    public ImmutablePercentileSampleWindow(
            long minRtt,
            int maxInFlight,
            boolean didDrop,
            PriorityQueue<Long> sortedRtts,
            int percentile
    ) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.didDrop = didDrop;
        this.sortedRtts = sortedRtts;
        this.percentile = percentile;
    }

    @Override
    public ImmutablePercentileSampleWindow addSample(long rtt, int maxInFlight) {
        // TODO: very naive
        // full copy in order to fulfill side-effect-free requirement of AtomicReference::updateAndGet
        PriorityQueue<Long> newSortedRtts = new PriorityQueue<>(sortedRtts);
        newSortedRtts.add(rtt);
        return new ImmutablePercentileSampleWindow(
                Math.min(minRtt, rtt),
                Math.max(maxInFlight, this.maxInFlight),
                didDrop,
                newSortedRtts,
                percentile
        );
    }

    @Override
    public ImmutablePercentileSampleWindow addDroppedSample(int maxInFlight) {
        return new ImmutablePercentileSampleWindow(
                minRtt,
                Math.max(maxInFlight, this.maxInFlight),
                didDrop,
                sortedRtts,
                percentile
        );
    }

    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getTrackedRttNanos() {
        int size = sortedRtts.size() - 1;
        int percentileIndex = (int) Math.round(size * percentile / 100.0);
        return sortedRtts.toArray(new Long[]{})[percentileIndex];
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

    @Override
    public ImmutablePercentileSampleWindow createBlankInstance() {
        return new ImmutablePercentileSampleWindow(percentile);
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

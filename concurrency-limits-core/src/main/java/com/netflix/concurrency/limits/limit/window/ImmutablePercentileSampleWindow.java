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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

class ImmutablePercentileSampleWindow implements SampleWindow {
    private final long minRtt;
    private final int maxInFlight;
    private final boolean didDrop;
    private final List<Long> observedRtts;
    private final double percentile;

    ImmutablePercentileSampleWindow(double percentile) {
        this.minRtt = Long.MAX_VALUE;
        this.maxInFlight = 0;
        this.didDrop = false;
        this.observedRtts = new ArrayList<>();
        this.percentile = percentile;
    }

    ImmutablePercentileSampleWindow(
            long minRtt,
            int maxInFlight,
            boolean didDrop,
            List<Long> observedRtts,
            double percentile
    ) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.didDrop = didDrop;
        this.observedRtts = observedRtts;
        this.percentile = percentile;
    }

    @Override
    public ImmutablePercentileSampleWindow addSample(long rtt, int inflight, boolean didDrop) {
        // TODO: very naive
        // full copy in order to fulfill side-effect-free requirement of AtomicReference::updateAndGet
        List<Long> newObservedRtts = new ArrayList<>(observedRtts);
        newObservedRtts.add(rtt);
        return new ImmutablePercentileSampleWindow(
                Math.min(minRtt, rtt),
                Math.max(inflight, this.maxInFlight),
                this.didDrop || didDrop,
                newObservedRtts,
                percentile
        );
    }

    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getTrackedRttNanos() {
        observedRtts.sort(Comparator.naturalOrder());
        int rttIndex = (int) Math.round(observedRtts.size() * percentile);
        int zeroBasedRttIndex = rttIndex - 1;
        return observedRtts.get(zeroBasedRttIndex);
    }

    @Override
    public int getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public int getSampleCount() {
        return observedRtts.size();
    }

    @Override
    public boolean didDrop() {
        return didDrop;
    }

    @Override
    public String toString() {
        return "ImmutablePercentileSampleWindow ["
                + "minRtt=" + TimeUnit.NANOSECONDS.toMicros(minRtt) / 1000.0
                + ", p" + percentile + " rtt=" + TimeUnit.NANOSECONDS.toMicros(getTrackedRttNanos()) / 1000.0
                + ", maxInFlight=" + maxInFlight
                + ", sampleCount=" + observedRtts.size()
                + ", didDrop=" + didDrop + "]";
    }
}

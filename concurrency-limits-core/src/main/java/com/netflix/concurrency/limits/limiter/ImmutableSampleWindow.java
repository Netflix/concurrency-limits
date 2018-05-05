package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;

import java.util.concurrent.TimeUnit;

/**
 * Class used to track immutable samples in an AtomicReference
 */
public class ImmutableSampleWindow implements Limit.SampleWindow {
    final long minRtt;
    final int maxInFlight;
    final int sampleCount;
    final boolean didDrop;
    
    public ImmutableSampleWindow() {
        this.minRtt = Long.MAX_VALUE;
        this.maxInFlight = 0;
        this.sampleCount = 0;
        this.didDrop = false;
    }
    
    public ImmutableSampleWindow(long minRtt, int maxInFlight, int sampleCount, boolean didDrop) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.sampleCount = sampleCount;
        this.didDrop = didDrop;
    }
    
    public ImmutableSampleWindow addSample(long rtt, int maxInFlight) {
        return new ImmutableSampleWindow(Math.min(rtt, minRtt), Math.max(maxInFlight, this.maxInFlight), sampleCount+1, didDrop);
    }
    
    public ImmutableSampleWindow addDroppedSample(int maxInFlight) {
        return new ImmutableSampleWindow(minRtt, Math.max(maxInFlight, this.maxInFlight), sampleCount, true);
    }
    
    @Override
    public long getCandidateRttNanos() {
        return minRtt;
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

    @Override
    public String toString() {
        return "ImmutableSample [minRtt=" + TimeUnit.NANOSECONDS.toMicros(minRtt) / 1000.0 + ", maxInFlight=" + maxInFlight + ", sampleCount=" + sampleCount
                + ", didDrop=" + didDrop + "]";
    }
}
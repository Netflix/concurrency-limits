package com.netflix.concurrency.limits.limiter;

import java.util.concurrent.TimeUnit;

import com.netflix.concurrency.limits.Limit;

/**
 * Class used to track immutable samples in an AtomicReference
 */
public class ImmutableSampleWindow implements Limit.SampleWindow {
    final long minRtt;
    final long sum;
    final int maxInFlight;
    final int sampleCount;
    final boolean didDrop;
    
    public ImmutableSampleWindow() {
        this.minRtt = Long.MAX_VALUE;
        this.sum = 0;
        this.maxInFlight = 0;
        this.sampleCount = 0;
        this.didDrop = false;
    }
    
    public ImmutableSampleWindow(long minRtt, long sum, int maxInFlight, int sampleCount, boolean didDrop) {
        this.minRtt = minRtt;
        this.sum = sum;
        this.maxInFlight = maxInFlight;
        this.sampleCount = sampleCount;
        this.didDrop = didDrop;
    }
    
    public ImmutableSampleWindow addSample(long rtt, int maxInFlight) {
        return new ImmutableSampleWindow(Math.min(rtt, minRtt), sum + rtt, Math.max(maxInFlight, this.maxInFlight), sampleCount+1, didDrop);
    }
    
    public ImmutableSampleWindow addDroppedSample(int maxInFlight) {
        return new ImmutableSampleWindow(minRtt, sum, Math.max(maxInFlight, this.maxInFlight), sampleCount, true);
    }
    
    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getRttSumNanos() {
        return sum;
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
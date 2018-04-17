package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;

/**
 * Class used to track immutable samples in an AtomicReference
 */
public class ImmutableSample implements Limit.SampleWindow {
    final long minRtt;
    final int maxInFlight;
    final int sampleCount;
    final boolean didDrop;
    
    public ImmutableSample() {
        this.minRtt = Integer.MAX_VALUE;
        this.maxInFlight = 0;
        this.sampleCount = 0;
        this.didDrop = false;
    }
    
    public ImmutableSample reset() {
        return new ImmutableSample();
    }

    public ImmutableSample(long minRtt, int maxInFlight, int sampleCount, boolean didDrop) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.sampleCount = sampleCount;
        this.didDrop = didDrop;
    }
    
    public ImmutableSample addSample(long rtt, int maxInFlight) {
        return new ImmutableSample(Math.min(rtt, minRtt), Math.max(maxInFlight, this.maxInFlight), sampleCount+1, didDrop);
    }
    
    public ImmutableSample addDroppedSample(int maxInFlight) {
        return new ImmutableSample(minRtt, Math.max(maxInFlight, this.maxInFlight), sampleCount+1, true);
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
}
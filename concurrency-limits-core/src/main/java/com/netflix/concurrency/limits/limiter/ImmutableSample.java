package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;

/**
 * Class used to track immutable samples in an AtomicReference
 */
public class ImmutableSample implements Limit.SampleWindow {
    final long minRtt;
    final long maxInFlight;
    final long sampleCount;
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

    public ImmutableSample(long minRtt, long maxInFlight, long sampleCount, boolean didDrop) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.sampleCount = sampleCount;
        this.didDrop = didDrop;
    }
    
    public ImmutableSample addSample(long rtt, long maxInFlight) {
        return new ImmutableSample(Math.min(rtt, minRtt), Math.max(maxInFlight, this.maxInFlight), sampleCount+1, didDrop);
    }
    
    public ImmutableSample addDroppedSample(long maxInFlight) {
        return new ImmutableSample(minRtt, Math.max(maxInFlight, this.maxInFlight), sampleCount+1, true);
    }
    
    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public long getSampleCount() {
        return sampleCount;
    }

    @Override
    public boolean didDrop() {
        return didDrop;
    }
}
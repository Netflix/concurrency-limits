package com.netflix.concurrency.limits.limit.window;

import org.junit.Assert;
import org.junit.Test;

public class ImmutablePercentileSampleWindowTest {
    private final long slowestRtt = 5000;
    private final long moderateRtt = 500;
    private final long fastestRtt = 10;

    @Test
    public void calculateP50() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = window.addSample(slowestRtt, 1);
        window = window.addSample(moderateRtt, 1);
        window = window.addSample(fastestRtt, 1);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldNotChangeTrackedRtt() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = window.addSample(slowestRtt, 1);
        window = window.addSample(moderateRtt, 1);
        window = window.addSample(fastestRtt, 1);
        window = window.addDroppedSample(1);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }
    
    @Test
    public void p999ReturnsSlowestObservedRtt() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.999);
        window = window.addSample(slowestRtt, 1);
        window = window.addSample(moderateRtt, 1);
        window = window.addSample(fastestRtt, 1);
        window = window.addDroppedSample(1);
        Assert.assertEquals(slowestRtt, window.getTrackedRttNanos());
    }
}

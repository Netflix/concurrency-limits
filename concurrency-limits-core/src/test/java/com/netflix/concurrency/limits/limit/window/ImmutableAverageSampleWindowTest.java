package com.netflix.concurrency.limits.limit.window;

import org.junit.Assert;
import org.junit.Test;

public class ImmutableAverageSampleWindowTest {
    @Test
    public void calculateAverage() {
        ImmutableAverageSampleWindow window = new ImmutableAverageSampleWindow();
        long bigRtt = 5000;
        long moderateRtt = 500;
        long lowRtt = 10;
        window = window.addSample(bigRtt, 1);
        window = window.addSample(moderateRtt, 1);
        window = window.addSample(lowRtt, 1);
        Assert.assertEquals((bigRtt + moderateRtt + lowRtt) / 3, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldNotChangeTrackedAverage() {
        ImmutableAverageSampleWindow window = new ImmutableAverageSampleWindow();
        long bigRtt = 5000;
        long moderateRtt = 500;
        long lowRtt = 10;
        window = window.addSample(bigRtt, 1);
        window = window.addSample(moderateRtt, 1);
        window = window.addSample(lowRtt, 1);
        window = window.addDroppedSample(1);
        Assert.assertEquals((bigRtt + moderateRtt + lowRtt) / 3, window.getTrackedRttNanos());
    }
}

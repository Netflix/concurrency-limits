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

import org.junit.Assert;
import org.junit.Test;


public class ImmutablePercentileSampleWindowTest {
    private final long bigRtt = 5000;
    private final long moderateRtt = 500;
    private final long lowRtt = 10;

    @Test
    public void calculateP50() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.5, 10);
        window = window.addSample(bigRtt, 0, false);
        window = window.addSample(moderateRtt, 0, false);
        window = window.addSample(lowRtt, 0, false);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldChangeTrackedRtt() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5, 10);
        window = window.addSample(lowRtt, 1, false);
        window = window.addSample(bigRtt, 1, true);
        window = window.addSample(bigRtt, 1, true);
        Assert.assertEquals(bigRtt, window.getTrackedRttNanos());
    }
    
    @Test
    public void p999ReturnsSlowestObservedRtt() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.999, 10);
        window = window.addSample(bigRtt, 1, false);
        window = window.addSample(moderateRtt, 1, false);
        window = window.addSample(lowRtt, 1, false);
        Assert.assertEquals(bigRtt, window.getTrackedRttNanos());
    }

    @Test
    public void rttObservationOrderDoesntAffectResultValue() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.999, 10);
        window = window.addSample(moderateRtt, 1, false);
        window = window.addSample(lowRtt, 1, false);
        window = window.addSample(bigRtt, 1, false);
        window = window.addSample(lowRtt, 1, false);
        Assert.assertEquals(bigRtt, window.getTrackedRttNanos());
    }

    @Test
    public void lowPercentilesReturnMinimumForNonEmptyWindows() {
        for (double percentile : new double[] {Double.MIN_VALUE, 0.01, 0.1, 0.49}) {
            SampleWindow window = PercentileSampleWindowFactory.of(percentile, 10).newInstance();
            Assert.assertEquals(0, window.getTrackedRttNanos());
            window = window.addSample(bigRtt, 1, false);
            Assert.assertEquals(bigRtt, window.getTrackedRttNanos());
        }
    }

    @Test
    public void percentileRankIsClampedAtTheFirstSample() {
        double[] percentiles = {0.01, 0.049, 0.05, 0.1, 0.149, 0.15, 0.5, 0.999};
        long[] expectedRtts = {10, 10, 10, 10, 10, 20, 50, 100};
        for (int i = 0; i < percentiles.length; i++) {
            double percentile = percentiles[i];
            SampleWindow window = PercentileSampleWindowFactory.of(percentile, 10).newInstance();
            long[] samples = {90, 30, 70, 10, 100, 50, 20, 80, 40, 60};
            for (long sample : samples) {
                window = window.addSample(sample, 1, false);
            }
            Assert.assertEquals("percentile " + percentile, expectedRtts[i], window.getTrackedRttNanos());
            Assert.assertEquals(expectedRtts[i], window.getTrackedRttNanos());
        }
    }
}

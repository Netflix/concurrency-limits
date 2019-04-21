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

import static com.netflix.concurrency.limits.limit.window.SampleWindowTestingUtils.addSample;

public class ImmutablePercentileSampleWindowTest {
    private final long slowestRtt = 5000;
    private final long moderateRtt = 500;
    private final long fastestRtt = 10;

    @Test
    public void calculateP50() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = addSample(window, slowestRtt);
        window = addSample(window, moderateRtt);
        window = addSample(window, fastestRtt);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldNotChangeTrackedRtt() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = addSample(window, slowestRtt);
        window = addSample(window, moderateRtt);
        window = addSample(window, fastestRtt);
        window = window.addDroppedSample(1);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }
    
    @Test
    public void p999ReturnsSlowestObservedRtt() {
        SampleWindow window = new ImmutablePercentileSampleWindow(0.999);
        window = addSample(window, slowestRtt);
        window = addSample(window, moderateRtt);
        window = addSample(window, fastestRtt);
        window = window.addDroppedSample(1);
        Assert.assertEquals(slowestRtt, window.getTrackedRttNanos());
    }

    @Test
    public void addingSamplesIsIdempotent() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = window.addSample(slowestRtt, 1, 1);
        window = window.addSample(moderateRtt, 2, 1);
        window = window.addSample(fastestRtt, 3, 1);
        window = window.addSample(fastestRtt, 3, 1);
        window = window.addSample(fastestRtt, 3, 1);
        window = window.addSample(fastestRtt, 3, 1);
        window = window.addSample(fastestRtt, 3, 1);
        window = window.addDroppedSample(1);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }
}

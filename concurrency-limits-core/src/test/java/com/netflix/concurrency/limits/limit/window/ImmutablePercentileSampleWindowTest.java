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
    private final long slowestRtt = 5000;
    private final long moderateRtt = 500;
    private final long fastestRtt = 10;

    @Test
    public void calculateP50() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = window.addSample(slowestRtt, 1, false);
        window = window.addSample(moderateRtt, 1, false);
        window = window.addSample(fastestRtt, 1, false);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldNotChangeTrackedRtt() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.5);
        window = window.addSample(slowestRtt, 1, false);
        window = window.addSample(moderateRtt, 1, false);
        window = window.addSample(fastestRtt, 1, false);
        window = window.addSample(slowestRtt, 1, true);
        Assert.assertEquals(moderateRtt, window.getTrackedRttNanos());
    }
    
    @Test
    public void p999ReturnsSlowestObservedRtt() {
        ImmutablePercentileSampleWindow window = new ImmutablePercentileSampleWindow(0.999);
        window = window.addSample(slowestRtt, 1, false);
        window = window.addSample(moderateRtt, 1, false);
        window = window.addSample(fastestRtt, 1, false);
        window = window.addSample(slowestRtt, 1, true);
        Assert.assertEquals(slowestRtt, window.getTrackedRttNanos());
    }
}

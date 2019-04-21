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

public class ImmutableAverageSampleWindowTest {
    private final long bigRtt = 5000;
    private final long moderateRtt = 500;
    private final long lowRtt = 10;

    @Test
    public void calculateAverage() {
        SampleWindow window = new ImmutableAverageSampleWindow();
        window = addSample(window, bigRtt);
        window = addSample(window, moderateRtt);
        window = addSample(window, lowRtt);
        Assert.assertEquals((bigRtt + moderateRtt + lowRtt) / 3, window.getTrackedRttNanos());
    }

    @Test
    public void droppedSampleShouldNotChangeTrackedAverage() {
        SampleWindow window = new ImmutableAverageSampleWindow();
        window = addSample(window, bigRtt);
        window = addSample(window, moderateRtt);
        window = addSample(window, lowRtt);
        window = window.addDroppedSample(1);
        Assert.assertEquals((bigRtt + moderateRtt + lowRtt) / 3, window.getTrackedRttNanos());
    }
}

/**
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class Gradient2LimitTest {
    @Test
    public void testRttCorrectionDuringWarmupSurvivesNextSample() {
        Gradient2Limit limit = Gradient2Limit.newBuilder()
                .initialLimit(100)
                .maxConcurrency(100)
                .queueSize(0)
                .smoothing(1.0)
                .rttTolerance(1.0)
                .longWindow(3)
                .build();

        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(1000), 100, false);
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(100), 100, false);
        // The 550 ms average exceeds twice the latest RTT, triggering a 5% correction.
        Assert.assertEquals(522500, limit.getRttNoLoad(TimeUnit.MICROSECONDS));

        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(300), 100, false);
        // With a three-sample exponential window, the corrected estimate has weight 1/2.
        Assert.assertEquals(411250, limit.getRttNoLoad(TimeUnit.MICROSECONDS));

        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(800), 100, false);
        Assert.assertEquals(605625, limit.getRttNoLoad(TimeUnit.MICROSECONDS));
        Assert.assertEquals(75, limit.getLimit());
    }
}

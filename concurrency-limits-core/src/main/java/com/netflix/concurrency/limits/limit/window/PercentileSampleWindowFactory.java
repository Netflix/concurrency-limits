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

import com.netflix.concurrency.limits.internal.Preconditions;

public class PercentileSampleWindowFactory implements SampleWindowFactory {
    private final double percentile;
    private final int windowSize;

    private PercentileSampleWindowFactory(double percentile, int windowSize) {
        this.percentile = percentile;
        this.windowSize = windowSize;
    }

    public static PercentileSampleWindowFactory of(double percentile, int windowSize) {
        Preconditions.checkArgument(percentile > 0 && percentile < 1.0, "Percentile should belong to (0, 1.0)");
        return new PercentileSampleWindowFactory(percentile, windowSize);
    }

    @Override
    public ImmutablePercentileSampleWindow newInstance() {
        return new ImmutablePercentileSampleWindow(percentile, (int) (windowSize * 1.2));
    }
}

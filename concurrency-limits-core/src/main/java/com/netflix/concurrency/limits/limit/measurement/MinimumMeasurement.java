/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.concurrency.limits.limit.measurement;

import java.util.function.Function;

public class MinimumMeasurement implements Measurement {
    private Double value = 0.0;
    
    @Override
    public Number add(Number sample) {
        if (value == 0.0 || sample.doubleValue() < value) {
            value = sample.doubleValue();
        }
        return value;
    }

    @Override
    public Number get() {
        return value;
    }

    @Override
    public void reset() {
        value = 0.0;
    }

    @Override
    public void update(Function<Number, Number> operation) {
        
    }
}

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
package com.netflix.concurrency.limits.internal;

import com.netflix.concurrency.limits.MetricRegistry;

import java.util.function.Supplier;

public final class EmptyMetricRegistry implements MetricRegistry {
    public static final EmptyMetricRegistry INSTANCE = new EmptyMetricRegistry();
    
    private EmptyMetricRegistry() {}
    
    @Override
    public SampleListener registerDistribution(String id, String... tagNameValuePairs) {
        return value -> { };
    }

    @Override
    public void registerGauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
    }
}

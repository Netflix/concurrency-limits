/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.dubbo;

import com.netflix.concurrency.limits.Limiter;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default limiter factory
 */
public class DubboLimiterFactoryImpl implements DubboLimiterFactory {
    private final ConcurrentMap<String, Limiter<Invocation>> limiters = new ConcurrentHashMap<>();

    @Override
    public Limiter<Invocation> getLimiter(URL url) {
        String key = url.getParameter(CommonConstants.SIDE_KEY, CommonConstants.CONSUMER_SIDE) + ":" + url.getPathKey();
        Limiter<Invocation> limiter = limiters.get(key);
        if (limiter != null) {
            return limiter;
        }
        limiters.put(key, createLimiter());
        limiter = limiters.get(key);
        return limiter;
    }

    private Limiter<Invocation> createLimiter() {
        return new DubboLimiterBuilder()
                .partitionByMethod()
                .blockOnLimit(true)
                .build();
    }
}

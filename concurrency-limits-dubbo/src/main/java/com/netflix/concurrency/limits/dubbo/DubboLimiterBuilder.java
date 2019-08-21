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
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import org.apache.dubbo.rpc.Invocation;

/**
 * Build limiter for dubbo
 */
public class DubboLimiterBuilder extends AbstractPartitionedLimiter.Builder<DubboLimiterBuilder, Invocation> {
    private boolean blockOnLimit = false;

    /**
     * Partition the limit by method
     *
     * @return Chainable builder
     */
    public DubboLimiterBuilder partitionByMethod() {
        return partitionResolver(Invocation::getMethodName);
    }

    /**
     * When set to true new calls to the channel will block when the limit has been reached instead
     * of failing fast with an UNAVAILABLE status.
     *
     * @param blockOnLimit
     * @return Chainable builder
     */
    public <T> DubboLimiterBuilder blockOnLimit(boolean blockOnLimit) {
        this.blockOnLimit = blockOnLimit;
        return this;
    }

    @Override
    protected DubboLimiterBuilder self() {
        return this;
    }

    public Limiter<Invocation> build() {
        Limiter<Invocation> limiter = super.build();

        if (blockOnLimit) {
            limiter = BlockingLimiter.wrap(limiter);
        }
        return limiter;
    }
}

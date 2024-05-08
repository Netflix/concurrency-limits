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
package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.internal.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Loss based dynamic {@link Limit} that does an additive increment as long as 
 * there are no errors and a multiplicative decrement when there is an error.
 */
public final class AIMDLimit extends AbstractLimit {
    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toNanos(5);
    private static final Logger log = LoggerFactory.getLogger(AIMDLimit.class);

    public static class Builder {
        private int minLimit = 20;
        private int initialLimit = 20;
        private int maxLimit = 200;
        private double backoffRatio = 0.9;
        private long timeout = DEFAULT_TIMEOUT;

        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }

        public Builder minLimit(int minLimit) {
            this.minLimit = minLimit;
            return this;
        }

        public Builder maxLimit(int maxLimit) {
            this.maxLimit = maxLimit;
            return this;
        }

        public Builder backoffRatio(double backoffRatio) {
            Preconditions.checkArgument(backoffRatio < 1.0 && backoffRatio >= 0.5, "Backoff ratio must be in the range [0.5, 1.0)");
            this.backoffRatio = backoffRatio;
            return this;
        }

        /**
         * Timeout threshold that when exceeded equates to a drop.
         * @param timeout
         * @param units
         * @return Chainable builder
         */
        public Builder timeout(long timeout, TimeUnit units) {
            Preconditions.checkArgument(timeout > 0, "Timeout must be positive");
            this.timeout = units.toNanos(timeout);
            return this;
        }
        
        public AIMDLimit build() {
            if (initialLimit > maxLimit) {
                log.warn("Initial limit {} exceeded maximum limit {}", initialLimit, maxLimit);
            }
            return new AIMDLimit(this);
        }
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    private final double backoffRatio;
    private final long timeout;
    private final int minLimit;
    private final int maxLimit;

    private AIMDLimit(Builder builder) {
        super(builder.initialLimit);
        this.backoffRatio = builder.backoffRatio;
        this.timeout = builder.timeout;
        this.maxLimit = builder.maxLimit;
        this.minLimit = builder.minLimit;
    }
    
    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        int currentLimit = getLimit();

        if (didDrop || rtt > timeout) {
            currentLimit = (int) (currentLimit * backoffRatio);
        } else if (inflight * 2 >= currentLimit) {
            currentLimit =  currentLimit + 1;
        }

        return Math.min(maxLimit, Math.max(minLimit, currentLimit));
    }

    @Override
    public String toString() {
        return "AIMDLimit [limit=" + getLimit() + "]";
    }
}

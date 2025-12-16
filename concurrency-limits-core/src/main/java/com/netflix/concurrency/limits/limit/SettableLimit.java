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

/**
 * {@link Limit} to be used mostly for testing where the limit can be manually
 * adjusted.
 */
public class SettableLimit extends AbstractLimit {

    /**
     * @deprecated use {@link #newBuilder()} instead
     */
    public static SettableLimit startingAt(int limit) {
        return new SettableLimit(limit);
    }

    /**
     * @deprecated use {@link #newBuilder()} instead
     */
    public SettableLimit(int limit) {
        this(new Builder().initialLimit(limit));
    }

    private SettableLimit(Builder builder) {
        super(builder);
    }

    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        return getLimit();
    }
    
    public synchronized void setLimit(int limit) {
        super.setLimit(limit);
    }

    @Override
    public String toString() {
        return "SettableLimit [limit=" + getLimit() + "]";
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends AbstractLimit.Builder<Builder> {

        public Builder() {
            super(-1);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public SettableLimit build() {
            return new SettableLimit(this);
        }
    }
}

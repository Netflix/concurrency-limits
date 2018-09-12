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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public abstract class AbstractLimit implements Limit {
    private volatile int limit;
    private final List<Consumer<Integer>> listeners = new CopyOnWriteArrayList<>();

    protected AbstractLimit(int initialLimit) {
        this.limit = initialLimit;
    }

    @Override
    public final synchronized void onSample(long startTime, long rtt, int inflight, boolean didDrop) {
        setLimit(_update(startTime, rtt, inflight, didDrop));
    }

    protected abstract int _update(long startTime, long rtt, int inflight, boolean didDrop);

    @Override
    public final int getLimit() {
        return limit;
    }

    protected synchronized void setLimit(int newLimit) {
        if (newLimit != limit) {
            limit = newLimit;
            listeners.forEach(listener -> listener.accept(newLimit));
        }
    }

    public void notifyOnChange(Consumer<Integer> consumer) {
        this.listeners.add(consumer);
    }


}

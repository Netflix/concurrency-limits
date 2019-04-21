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
import com.netflix.concurrency.limits.limit.window.AverageSampleWindowFactory;
import com.netflix.concurrency.limits.limit.window.SampleWindow;
import com.netflix.concurrency.limits.limit.window.SampleWindowFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WindowedLimit implements Limit {
    private static final long DEFAULT_MIN_WINDOW_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_MAX_WINDOW_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_MIN_RTT_THRESHOLD = TimeUnit.MICROSECONDS.toNanos(100);

    /**
     * Minimum observed samples to filter out sample windows with not enough significant samples
     */
    private static final int DEFAULT_WINDOW_SIZE = 10;

    private final AtomicLong sequence = new AtomicLong();

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long maxWindowTime = DEFAULT_MAX_WINDOW_TIME;
        private long minWindowTime = DEFAULT_MIN_WINDOW_TIME;
        private int windowSize = DEFAULT_WINDOW_SIZE;
        private long minRttThreshold = DEFAULT_MIN_RTT_THRESHOLD;
        private SampleWindowFactory sampleWindowFactory = AverageSampleWindowFactory.create();

        /**
         * Minimum window duration for sampling a new minRtt
         */
        public Builder minWindowTime(long minWindowTime, TimeUnit units) {
            Preconditions.checkArgument(units.toMillis(minWindowTime) >= 100, "minWindowTime must be >= 100 ms");
            this.minWindowTime = units.toNanos(minWindowTime);
            return this;
        }

        /**
         * Maximum window duration for sampling a new minRtt
         */
        public Builder maxWindowTime(long maxWindowTime, TimeUnit units) {
            Preconditions.checkArgument(units.toMillis(maxWindowTime) >= 100, "maxWindowTime must be >= 100 ms");
            this.maxWindowTime = units.toNanos(maxWindowTime);
            return this;
        }

        /**
         * Minimum sampling window size for finding a new minimum rtt
         */
        public Builder windowSize(int windowSize) {
            Preconditions.checkArgument(windowSize >= 10, "Window size must be >= 10");
            this.windowSize = windowSize;
            return this;
        }

        public Builder minRttThreshold(long threshold, TimeUnit units) {
            this.minRttThreshold = units.toNanos(threshold);
            return this;
        }

        public Builder sampleWindowFactory(SampleWindowFactory sampleWindowFactory) {
            this.sampleWindowFactory = sampleWindowFactory;
            return this;
        }

        public WindowedLimit build(Limit delegate) {
            return new WindowedLimit(this, delegate);
        }
    }

    private final Limit delegate;

    /**
     * End time for the sampling window at which point the limit should be updated
     */
    private volatile long nextUpdateTime = 0;

    private final long minWindowTime;

    private final long maxWindowTime;

    private final int windowSize;

    private final long minRttThreshold;

    private final Object lock = new Object();

    private final SampleWindowFactory sampleWindowFactory;

    /**
     * Object tracking stats for the current sample window
     */
    private final AtomicReference<SampleWindow> sample;

    private WindowedLimit(Builder builder, Limit delegate) {
        this.delegate = delegate;
        this.minWindowTime = builder.minWindowTime;
        this.maxWindowTime = builder.maxWindowTime;
        this.windowSize = builder.windowSize;
        this.minRttThreshold = builder.minRttThreshold;
        this.sampleWindowFactory = builder.sampleWindowFactory;
        this.sample = new AtomicReference<>(sampleWindowFactory.newInstance());
    }

    @Override
    public void notifyOnChange(Consumer<Integer> consumer) {
        delegate.notifyOnChange(consumer);
    }

    @Override
    public void onSample(long startTime, long rtt, int inflight, boolean didDrop) {
        long endTime = startTime + rtt;

        if (rtt < minRttThreshold) {
            return;
        }

        sequence.incrementAndGet();
        if (didDrop) {
            sample.updateAndGet(current -> current.addDroppedSample(inflight));
        } else {
            sample.updateAndGet(current -> current.addSample(rtt, sequence.get(), inflight));
        }

        if (startTime + rtt > nextUpdateTime) {
            synchronized (lock) {
                // Double check under the lock
                if (endTime > nextUpdateTime) {
                    SampleWindow current = sample.get();
                    if (isWindowReady(current)) {
                        sample.set(sampleWindowFactory.newInstance());

                        nextUpdateTime = endTime + Math.min(Math.max(current.getCandidateRttNanos() * 2, minWindowTime), maxWindowTime);
                        delegate.onSample(startTime, current.getTrackedRttNanos(), current.getMaxInFlight(), current.didDrop());
                    }
                }
            }
        }
    }

    private boolean isWindowReady(SampleWindow sample) {
        return sample.getCandidateRttNanos() < Long.MAX_VALUE && sample.getSampleCount() > windowSize;
    }

    @Override
    public int getLimit() {
        return delegate.getLimit();
    }
}

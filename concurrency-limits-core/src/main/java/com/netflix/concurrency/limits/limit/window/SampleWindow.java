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

/**
 * Implementations of this interface are being used to track immutable samples in an AtomicReference
 *
 * @see com.netflix.concurrency.limits.limit.WindowedLimit
 */
public interface SampleWindow {
    SampleWindow addSample(long rtt, long seqId, int inflight);

    SampleWindow addDroppedSample(int inflight);

    long getCandidateRttNanos();

    long getTrackedRttNanos();

    int getMaxInFlight();

    int getSampleCount();

    boolean didDrop();
}

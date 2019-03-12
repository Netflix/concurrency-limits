package com.netflix.concurrency.limits.limit.window;

public interface SampleWindowFactory<T extends SampleWindow> {
    T newInstance();

    T addDroppedSample(T sampleWindow, int inflight);

    T addSample(T sampleWindow, long rtt, int inflight);
}

package com.netflix.concurrency.limits.limit.window;

import java.util.concurrent.atomic.AtomicLong;

public class SampleWindowTestingUtils {
    private static final AtomicLong SEQ = new AtomicLong();

    public static SampleWindow addSample(SampleWindow sampleWindow, long rtt) {
        return sampleWindow.addSample(rtt, SEQ.incrementAndGet(), 1);
    }
}

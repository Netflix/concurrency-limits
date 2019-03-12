package com.netflix.concurrency.limits.limit.window;

import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.PriorityQueue;

public class PercentileSampleWindowFactory implements SampleWindowFactory<ImmutablePercentileSampleWindow> {
    private final double percentile;

    private PercentileSampleWindowFactory(double percentile) {
        this.percentile = percentile;
    }

    public static PercentileSampleWindowFactory of(double percentile) {
        Preconditions.checkArgument(percentile > 0 && percentile < 1.0, "Percentile should belong to (0, 1.0)");
        return new PercentileSampleWindowFactory(percentile);
    }

    @Override
    public ImmutablePercentileSampleWindow newInstance() {
        return new ImmutablePercentileSampleWindow(percentile);
    }

    @Override
    public ImmutablePercentileSampleWindow addDroppedSample(ImmutablePercentileSampleWindow sampleWindow, int inflight) {
        return new ImmutablePercentileSampleWindow(
                sampleWindow.getCandidateRttNanos(),
                Math.max(inflight, sampleWindow.getMaxInFlight()),
                true,
                sampleWindow.getSortedRtts(),
                percentile
        );
    }

    @Override
    public ImmutablePercentileSampleWindow addSample(ImmutablePercentileSampleWindow sampleWindow, long rtt, int inflight) {
        // TODO: very naive
        // full copy in order to fulfill side-effect-free requirement of AtomicReference::updateAndGet
        PriorityQueue<Long> newSortedRtts = new PriorityQueue<>(sampleWindow.getSortedRtts());
        newSortedRtts.add(rtt);
        return new ImmutablePercentileSampleWindow(
                Math.min(sampleWindow.getCandidateRttNanos(), rtt),
                Math.max(inflight, sampleWindow.getMaxInFlight()),
                sampleWindow.didDrop(),
                newSortedRtts,
                percentile
        );
    }
}

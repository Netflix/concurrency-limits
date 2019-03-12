package com.netflix.concurrency.limits.limit.window;

public class AverageSampleWindowFactory implements SampleWindowFactory<ImmutableAverageSampleWindow> {
    private AverageSampleWindowFactory() {}

    public static AverageSampleWindowFactory create() {
        return new AverageSampleWindowFactory();
    }

    @Override
    public ImmutableAverageSampleWindow newInstance() {
        return new ImmutableAverageSampleWindow();
    }

    @Override
    public ImmutableAverageSampleWindow addDroppedSample(ImmutableAverageSampleWindow sampleWindow, int inflight) {
        return new ImmutableAverageSampleWindow(
                sampleWindow.getCandidateRttNanos(),
                sampleWindow.getSum(),
                Math.max(inflight, sampleWindow.getMaxInFlight()),
                sampleWindow.getSampleCount(),
                true
        );
    }

    @Override
    public ImmutableAverageSampleWindow addSample(ImmutableAverageSampleWindow sampleWindow, long rtt, int inflight) {
        return new ImmutableAverageSampleWindow(
                Math.min(rtt, sampleWindow.getCandidateRttNanos()),
                sampleWindow.getSum() + rtt,
                Math.max(inflight, sampleWindow.getMaxInFlight()),
                sampleWindow.getSampleCount() + 1,
                sampleWindow.didDrop()
        );
    }
}

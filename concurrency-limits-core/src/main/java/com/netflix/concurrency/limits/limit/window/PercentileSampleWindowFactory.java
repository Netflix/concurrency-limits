package com.netflix.concurrency.limits.limit.window;

import com.netflix.concurrency.limits.internal.Preconditions;

public class PercentileSampleWindowFactory implements SampleWindowFactory {
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
}

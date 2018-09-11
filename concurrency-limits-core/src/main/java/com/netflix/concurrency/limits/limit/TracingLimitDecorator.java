package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TracingLimitDecorator implements Limit {
    private static final Logger LOG = LoggerFactory.getLogger(TracingLimitDecorator.class);
    
    private final Limit delegate;

    public static TracingLimitDecorator wrap(Limit delegate) {
        return new TracingLimitDecorator(delegate);
    }
    
    public TracingLimitDecorator(Limit delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public int getLimit() {
        return delegate.getLimit();
    }

    @Override
    public void onSample(long startTime, long rtt, int inflight, boolean didDrop) {
        LOG.debug("maxInFlight={} minRtt={} ms",
                inflight,
                TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0);
        delegate.onSample(startTime, rtt, inflight, didDrop);
    }

    @Override
    public void notifyOnChange(Consumer<Integer> consumer) {
        delegate.notifyOnChange(consumer);
    }
}

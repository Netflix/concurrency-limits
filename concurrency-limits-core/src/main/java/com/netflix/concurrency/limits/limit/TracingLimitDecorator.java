package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;

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
    public void update(SampleWindow sample) {
        LOG.debug("sampleCount={} maxInFlight={} minRtt={} ms", 
                sample.getSampleCount(),
                sample.getMaxInFlight(),
                TimeUnit.NANOSECONDS.toMicros(sample.getCandidateRttNanos()) / 1000.0);
        delegate.update(sample);
    }

}

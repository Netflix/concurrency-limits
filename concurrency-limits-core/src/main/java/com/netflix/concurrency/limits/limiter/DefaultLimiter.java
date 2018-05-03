package com.netflix.concurrency.limits.limiter;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.Strategy.Token;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limit.VegasLimit;

/**
 * {@link Limiter} that combines a plugable limit algorithm and enforcement strategy to 
 * enforce concurrency limits to a fixed resource.  
 * @param <ContextT> 
 */
public final class DefaultLimiter<ContextT> implements Limiter<ContextT> {
    private final Supplier<Long> nanoClock = System::nanoTime;
    
    private static final long DEFAULT_MIN_WINDOW_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final int DEFAULT_WINDOW_SIZE = 10;

    /**
     * Minimum observed samples to filter out sample windows with not enough significant samples
     */
    private static final int MIN_WINDOW_SAMPLE_COUNT = 10;
    
    /**
     * Minimum observed max inflight to filter out sample windows with not enough significant data
     */
    private static final int MIN_WINDOW_MAX_INFLIGHT = 1;
    
    /**
     * End time for the sampling window at which point the limit should be updated
     */
    private final AtomicLong nextUpdateTime = new AtomicLong();

    /**
     * Algorithm used to determine the new limit based on the current limit and minimum
     * measured RTT in the sample window
     */
    private final Limit limit;

    /**
     * Strategy for enforcing the limit
     */
    private final Strategy<ContextT> strategy;
    
    /**
     * Minimum window size in nanonseconds for sampling a new minRtt
     */
    private final long minWindowTime;
    
    /**
     * Sampling window size in multiple of the measured minRtt
     */
    private final int windowSize;
    
    /**
     * Object tracking stats for the current sample window
     */
    private final AtomicReference<ImmutableSample> sample = new AtomicReference<>(new ImmutableSample());
    
    /**
     * Counter tracking the current number of inflight requests
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    public static class Builder {
        private Limit limit = VegasLimit.newDefault();
        private long minWindowTime = DEFAULT_MIN_WINDOW_TIME;
        private int windowSize = DEFAULT_WINDOW_SIZE;
        
        public Builder limit(Limit limit) {
            Preconditions.checkArgument(limit != null, "Algorithm may not be null");
            this.limit = limit;
            return this;
        }
        
        public Builder minWindowTime(long minWindowTime, TimeUnit units) {
            Preconditions.checkArgument(units.toMillis(minWindowTime) >= 100, "minWindowTime must be >= 100 ms");
            this.minWindowTime = units.toNanos(minWindowTime);
            return this;
        }
        
        public Builder windowSize(int windowSize) {
            Preconditions.checkArgument(windowSize >= 10, "Window size must be >= 10");
            this.windowSize = windowSize;
            return this;
        }
        
        public <ContextT> DefaultLimiter<ContextT> build(Strategy<ContextT> strategy) {
            Preconditions.checkArgument(strategy != null, "Strategy may not be null");
            return new DefaultLimiter<ContextT>(this, strategy);
        }
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    /**
     * @deprecated Use {@link DefaultLimiter#newBuilder}
     * @param limit
     * @param strategy
     */
    @Deprecated
    public DefaultLimiter(Limit limit, Strategy<ContextT> strategy) {
        Preconditions.checkArgument(limit != null, "Algorithm may not be null");
        Preconditions.checkArgument(strategy != null, "Strategy may not be null");
        this.limit = limit;
        this.strategy = strategy;
        this.windowSize = DEFAULT_WINDOW_SIZE;
        this.minWindowTime = DEFAULT_MIN_WINDOW_TIME;
        strategy.setLimit(limit.getLimit());
    }
    
    private DefaultLimiter(Builder builder, Strategy<ContextT> strategy) {
        this.limit = builder.limit;
        this.minWindowTime = builder.minWindowTime;
        this.windowSize = builder.windowSize;
        this.strategy = strategy;
        strategy.setLimit(limit.getLimit());
    }

    @Override
    public Optional<Listener> acquire(final ContextT context) {
        // Did we exceed the limit
        final Token token = strategy.tryAcquire(context);
        if (!token.isAcquired()) {
            return Optional.empty();
        }
        
        final long startTime = nanoClock.get();
        int currentMaxInFlight = inFlight.incrementAndGet();

        return Optional.of(new Listener() {
            @Override
            public void onSuccess() {
                inFlight.decrementAndGet();
                token.release();
                
                final long endTime = nanoClock.get();
                final long rtt = endTime - startTime;
                
                sample.getAndUpdate(current -> current.addSample(rtt, currentMaxInFlight));
                
                long updateTime = nextUpdateTime.get();
                if (endTime >= updateTime) {
                    long nextUpdate = endTime + Math.max(minWindowTime, rtt * windowSize);
                    if (nextUpdateTime.compareAndSet(updateTime, nextUpdate)) {
                        ImmutableSample last = sample.getAndUpdate(ImmutableSample::reset);
                        if (last.getCandidateRttNanos() < Integer.MAX_VALUE
                            && last.getSampleCount() > MIN_WINDOW_SAMPLE_COUNT
                            && last.getMaxInFlight() > MIN_WINDOW_MAX_INFLIGHT) {
                            limit.update(last);
                            strategy.setLimit(limit.getLimit());
                        }
                    }
                }
            }
            
            @Override
            public void onIgnore() {
                token.release();
            }

            @Override
            public void onDropped() {
                inFlight.decrementAndGet();
                token.release();
                
                sample.getAndUpdate(current -> current.addDroppedSample(currentMaxInFlight));
            }
        });
    }
    
    protected int getLimit() {
        return limit.getLimit();
    }
    
    @Override
    public String toString() {
        return "DefaultLimiter [RTT_candidate=" + TimeUnit.NANOSECONDS.toMicros(sample.get().getCandidateRttNanos()) / 1000.0
                + ", maxInFlight=" + inFlight 
                + ", " + limit 
                + ", " + strategy
                + "]";
    }
}

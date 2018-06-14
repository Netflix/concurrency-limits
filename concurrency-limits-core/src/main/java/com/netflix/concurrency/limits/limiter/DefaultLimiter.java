package com.netflix.concurrency.limits.limiter;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final long DEFAULT_MAX_WINDOW_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final long DEFAULT_MIN_RTT_THRESHOLD = TimeUnit.MICROSECONDS.toNanos(100);
    
    /**
     * Minimum observed samples to filter out sample windows with not enough significant samples
     */
    private static final int DEFAULT_WINDOW_SIZE = 10;
    
    /**
     * End time for the sampling window at which point the limit should be updated
     */
    private volatile long nextUpdateTime = 0;

    private final Limit limit;

    private final Strategy<ContextT> strategy;
    
    private final long minWindowTime;
    
    private final long maxWindowTime;
    
    private final long minRttThreshold;
    
    private final int windowSize;
    
    /**
     * Object tracking stats for the current sample window
     */
    private final AtomicReference<ImmutableSampleWindow> sample = new AtomicReference<>(new ImmutableSampleWindow());
    
    /**
     * Counter tracking the current number of inflight requests
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    private final Object lock = new Object();
    
    public static class Builder {
        private Limit limit = VegasLimit.newDefault();
        private long maxWindowTime = DEFAULT_MAX_WINDOW_TIME;
        private long minWindowTime = DEFAULT_MIN_WINDOW_TIME;
        private int windowSize = DEFAULT_WINDOW_SIZE;
        private long minRttThreshold = DEFAULT_MIN_RTT_THRESHOLD;
        
        /**
         * Algorithm used to determine the new limit based on the current limit and minimum
         * measured RTT in the sample window
         */
        public Builder limit(Limit limit) {
            Preconditions.checkArgument(limit != null, "Algorithm may not be null");
            this.limit = limit;
            return this;
        }
        
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
            Preconditions.checkArgument(maxWindowTime >= units.toMillis(100), "minWindowTime must be >= 100 ms");
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
        
        public Builder minRttThreshold(long minRttThreshold, TimeUnit units) {
            this.minRttThreshold = units.toNanos(minRttThreshold);
            return this;
        }
        
        /**
         * @param strategy Strategy for enforcing the limit
         */
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
        this.maxWindowTime = DEFAULT_MAX_WINDOW_TIME;
        this.minRttThreshold = DEFAULT_MIN_RTT_THRESHOLD;
        strategy.setLimit(limit.getLimit());
    }
    
    private DefaultLimiter(Builder builder, Strategy<ContextT> strategy) {
        this.limit = builder.limit;
        this.minWindowTime = builder.minWindowTime;
        this.maxWindowTime = builder.maxWindowTime;
        this.minRttThreshold = DEFAULT_MIN_RTT_THRESHOLD;
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
                
                if (rtt > minRttThreshold) {
                    sample.updateAndGet(window -> window.addSample(rtt, currentMaxInFlight));
                }
                
                if (endTime > nextUpdateTime) {
                    synchronized (lock) {
                        // Double check under the lock
                        if (endTime > nextUpdateTime) {
                            ImmutableSampleWindow current = sample.get();
                            if (isWindowReady(current)) {
                                sample.set(new ImmutableSampleWindow());
                                
                                nextUpdateTime = endTime + Math.min(Math.max(current.getCandidateRttNanos() * 2, minWindowTime), maxWindowTime);
                                limit.update(current);
                                strategy.setLimit(limit.getLimit());
                            }
                        }
                    }
                }
            }
            
            @Override
            public void onIgnore() {
                inFlight.decrementAndGet();
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
    
    private boolean isWindowReady(ImmutableSampleWindow sample) {
        return sample.getCandidateRttNanos() < Long.MAX_VALUE && sample.getSampleCount() > windowSize;
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

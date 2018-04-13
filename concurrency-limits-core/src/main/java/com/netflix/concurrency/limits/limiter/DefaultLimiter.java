package com.netflix.concurrency.limits.limiter;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    
    private final static long DEFAULT_MIN_WINDOW_TIME = TimeUnit.SECONDS.toNanos(1);
    private final static int DEFAULT_WINDOW_SIZE = 10;
    
    /**
     * Ideal RTT when no queuing occurs.  For simplicity we assume the lowest latency
     * ever observed is the ideal RTT.
     */
    private final AtomicLong RTT_noload = new AtomicLong(Long.MAX_VALUE);
    
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
    private final AtomicLong inFlight = new AtomicLong();

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
            Preconditions.checkArgument(minWindowTime >= units.toMillis(100), "minWindowTime must be >= 100 ms");
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
        final long startTime = nanoClock.get();
        
        // Did we exceed the limit
        final Token token = strategy.tryAcquire(context);
        if (!token.isAcquired()) {
            return Optional.empty();
        }
        
        long currentMaxInFlight = inFlight.incrementAndGet();

        return Optional.of(new Listener() {
            @Override
            public void onSuccess() {
                inFlight.decrementAndGet();
                token.release();
                
                final long endTime = nanoClock.get();
                final long rtt = endTime - startTime;
                
                // Keep track of the absolute minimum RTT seen
                if (rtt < RTT_noload.get()) {
                    RTT_noload.set(rtt);
                }
                
                sample.getAndUpdate(current -> current.addSample(rtt, currentMaxInFlight));
                
                long updateTime = nextUpdateTime.get();
                if (endTime >= updateTime && nextUpdateTime.compareAndSet(updateTime, endTime + Math.max(minWindowTime, RTT_noload.get() * windowSize))) {
                    ImmutableSample last = sample.getAndUpdate(ImmutableSample::reset);
                    if (last.getCandidateRttNanos() < Integer.MAX_VALUE) {
                        limit.update(last);
                        strategy.setLimit(limit.getLimit());
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
    
    /**
     * @return Return the minimum observed RTT time or 0 if none found yet
     */
    protected long getMinRtt() {
        long current = RTT_noload.get();
        return current == Long.MAX_VALUE ? 0 : current;
    }

    @Override
    public String toString() {
        return "DefaultLimiter [RTT_noload=" + TimeUnit.NANOSECONDS.toMicros(getMinRtt()) / 1000.0
                + ", RTT_candidate=" + TimeUnit.NANOSECONDS.toMicros(sample.get().getCandidateRttNanos()) / 1000.0
                + ", maxInFlight=" + inFlight 
                + ", " + limit 
                + ", " + strategy
                + "]";
    }
}

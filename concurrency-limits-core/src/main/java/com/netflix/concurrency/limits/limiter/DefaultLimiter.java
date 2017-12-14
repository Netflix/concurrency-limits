package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * {@link Limiter} that combines a plugable limit algorithm and enforcement strategy to 
 * enforce concurrency limits to a fixed resource.  
 * @param <ContextT> 
 */
public final class DefaultLimiter<ContextT> implements Limiter<ContextT> {
    private final Supplier<Long> nanoClock = System::nanoTime;
    
    /**
     * Ideal RTT when no queuing occurs.  For simplicity we assume the lowest latency
     * ever observed is the ideal RTT.
     */
    private volatile long RTT_noload = TimeUnit.MILLISECONDS.toNanos(100);
    
    /**
     * Smallest observed RTT during the sampling window. 
     */
    private final AtomicLong RTT_candidate = new AtomicLong(Integer.MAX_VALUE);
    
    /**
     * Set to true if the concurrency limit was never reached during a sampling window.
     * This means that the application was not able to send enough requests to test 
     * the limit.  Limits are not adjusted when this happens as doing so would cause
     * the limit to drive infinitely upwards.
     */
    private volatile boolean isAppLimited = false;
    
    /**
     * End time for the sampling window at which point the limit should be updated
     */
    private final AtomicLong nextUpdateTime = new AtomicLong();

    /**
     * Algorithm used to determine the new limit based on the current limit and minimum
     * measured RTT in the sample window
     */
    private final Limit limit;

    private final Strategy<ContextT> strategy;
    
    public DefaultLimiter(Limit limit, Strategy<ContextT> strategy) {
        Preconditions.checkArgument(limit != null, "Algorithm may not be null");
        this.limit = limit;
        this.strategy = strategy;
        
        strategy.setLimit(limit.getLimit());
    }
    
    @Override
    public Optional<Listener> acquire(final ContextT context) {
        final long startTime = nanoClock.get();
        
        // Did we exceed the limit
        if (!strategy.tryAcquire(context)) {
            isAppLimited = false;
            return Optional.empty();
        }
        
        return Optional.of(new Listener() {
            @Override
            public void onSuccess() {
                strategy.release(context);
                
                final long endTime = nanoClock.get();
                long rtt = endTime - startTime;
                
                if (rtt < RTT_noload) {
                    RTT_noload = rtt;
                }
                
                long current = RTT_candidate.get();
                if (rtt < current) {
                    RTT_candidate.compareAndSet(current, rtt);
                    current = rtt;
                }
                
                long updateTime = nextUpdateTime.get();
                if (endTime >= updateTime && nextUpdateTime.compareAndSet(updateTime, endTime + RTT_noload * 10)) {
                    if (!isAppLimited) {
                        strategy.setLimit(limit.update(current));
                        isAppLimited = true;
                    }
                    RTT_candidate.set(Integer.MAX_VALUE);
                }
            }
            
            @Override
            public void onIgnore() {
                strategy.release(context);
            }

            @Override
            public void onDropped() {
                limit.drop();
            }
        });
    }
    
    protected int getLimit() {
        return limit.getLimit();
    }

    @Override
    public String toString() {
        return "DefaultLimiter [RTT_noload=" + TimeUnit.NANOSECONDS.toMillis(RTT_noload)
                + ", RTT_candidate=" + TimeUnit.NANOSECONDS.toMillis(RTT_candidate.get()) 
                + ", isAppLimited=" + isAppLimited 
                + ", " + limit + "]";
    }
}

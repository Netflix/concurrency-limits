package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builder to simplify creating a {@link GrpcClientLimiter} where portions of the limit are 
 * segmented. 
 * 
 * The aggregate percentage must not exceed 100%. 
 * Predicates are evaluated in the order they were configured.
 * No checks for overlaps are performed.  The first perdicate to match will be used.
 */
public class GrpcClientLimiterBuilder {
    private final List<Segment> bins = new ArrayList<>();
    private Limit limit = VegasLimit.newDefault();
    private boolean blockOnLimit = false;
    
    /**
     * Set the limit algorithm to use.  Default is {@link VegasLimit}
     * @param limit Limit algorithm to use
     * @return Chainable builder
     */
    public GrpcClientLimiterBuilder limit(Limit limit) {
        this.limit = limit;
        return this;
    }
    
    /**
     * When set to true new calls to the channel will block when the limit has been reached instead
     * of failing fast with an UNAVAILABLE status. 
     * @param blockOnLimit
     * @return Chainable builder
     */
    public <T> GrpcClientLimiterBuilder blockOnLimit(boolean blockOnLimit) {
        this.blockOnLimit = blockOnLimit;
        return this;
    }
    
    /**
     * Guarantee a percentage of the limit to the specific method
     * @param percent Percent of the limit to guarantee 
     * @param method The method
     * @return Chainable builder
     */
    public <T> GrpcClientLimiterBuilder byMethod(double d, MethodDescriptor<?, ?> m) {
        bins.add(new Segment(d, (method, callOptions) -> method.getFullMethodName().equals(method.getFullMethodName())));
        return this;
    }
    
    public GrpcClientLimiter build() {
        if (bins.size() == 0) {
            return buildSimpleLimiter(limit, blockOnLimit);
        } else {
            return buildPercentageLimiter(limit, blockOnLimit, new ArrayList<>(bins));
        }
    }
    
    private static GrpcClientLimiter buildSimpleLimiter(final Limit limit, boolean blockOnLimit) {
        final Limiter<Void> limiter = blockOnLimit 
                ? BlockingLimiter.wrap(new DefaultLimiter<>(limit, new SimpleStrategy()))
                : new DefaultLimiter<>(limit, new SimpleStrategy());
                
        return new GrpcClientLimiter() {
            @Override
            public Optional<Listener> acquire(MethodDescriptor<?, ?> method, CallOptions callOptions) {
                return limiter.acquire(null);
            }
        };
    }
        
    private static GrpcClientLimiter buildPercentageLimiter(final Limit limit, boolean blockOnLimit, final List<Segment> bins) {
        final PercentageStrategy strategy = new PercentageStrategy(
                Stream.concat(bins.stream().map(Segment::getPercent), Stream.of(0.0))
                      .collect(Collectors.toList()));
        
        final Limiter<Integer> limiter = blockOnLimit 
                ? BlockingLimiter.wrap(new DefaultLimiter<>(limit, strategy))
                : new DefaultLimiter<>(limit, strategy);
                
        final int defaultBinIndex = bins.size();
        
        return new GrpcClientLimiter() {
            @Override
            public Optional<Listener> acquire(MethodDescriptor<?, ?> method, CallOptions callOptions) {
                for (int i = 0; i < bins.size(); i++) {
                    if (bins.get(i).predicate.test(method, callOptions)) {
                        return limiter.acquire(i);
                    }
                }
                return limiter.acquire(defaultBinIndex);
            }
        };
    }

    private static class Segment {
        private final double percent;
        private final BiPredicate<MethodDescriptor<?, ?>, CallOptions> predicate;
        
        Segment(double percent, BiPredicate<MethodDescriptor<?, ?>, CallOptions> predicate) {
            this.percent = percent;
            this.predicate = predicate;
        }
        
        double getPercent() {
            return percent;
        }
    }
}

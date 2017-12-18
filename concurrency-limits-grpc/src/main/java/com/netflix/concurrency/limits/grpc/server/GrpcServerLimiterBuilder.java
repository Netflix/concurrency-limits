package com.netflix.concurrency.limits.grpc.server;

import com.google.common.base.Preconditions;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builder to simplify creating a {@link GrpcServerLimiter} where portions of the limit are 
 * segmented. 
 * 
 * The aggregate percentage must not exceed 100%. 
 * Predicates are evaluated in the order they were configured.
 * No checks for overlaps are performed.  The first perdicate to match will be used.
 */
public class GrpcServerLimiterBuilder {
    private final List<Segment> bins = new ArrayList<>();
    private Limit limit = VegasLimit.newDefault();
    
    /**
     * Set the limit algorithm to use.  Default is {@link VegasLimit}
     * @param limit Limit algorithm to use
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder limit(Limit limit) {
        Preconditions.checkArgument(limit != null, "limit cannot be null");
        this.limit = limit;
        return this;
    }
    
    /**
     * Guarantee a percentage of the limit when a header matches a specific value.
     * 
     * @param percent Percent of the limit to guarantee 
     * @param key Header to evaluate
     * @param value Header value to match
     * @return Chainable builder
     */
    public <T> GrpcServerLimiterBuilder headerEquals(double percent, Metadata.Key<T> key, T value) {
        Preconditions.checkArgument(key != null, "Key cannot be null");
        Preconditions.checkArgument(value != null, "Value cannot be null");
        bins.add(new Segment(percent, (call, headers) -> Optional.ofNullable(headers.get(key)).map(t -> t.equals(value)).orElse(false)));
        return this;
    }
    
    /**
     * Guarantee a percentage of the limit to the specific method
     * @param percent Percent of the limit to guarantee 
     * @param method The method
     * @return Chainable builder
     */
    public <T> GrpcServerLimiterBuilder methodEquals(double percent, MethodDescriptor<?, ?> method) {
        bins.add(new Segment(percent, (call, headers) -> call.getMethodDescriptor().getFullMethodName().equals(method.getFullMethodName())));
        return this;
    }
    
    public GrpcServerLimiter build() {
        if (bins.size() == 0) {
            return buildSimpleLimiter(limit);
        } else {
            return buildPercentageInternal(limit, new ArrayList<>(bins));
        }
    }

    private GrpcServerLimiter buildSimpleLimiter(final Limit limit) {
        final DefaultLimiter<Void> limiter = new DefaultLimiter<Void>(limit, new SimpleStrategy());
        return new GrpcServerLimiter() {
            @Override
            public Optional<Listener> acquire(ServerCall<?, ?> call, Metadata headers) {
                return limiter.acquire(null);
            }
        };
    }

    private GrpcServerLimiter buildPercentageInternal(Limit limit, final List<Segment> bins) {
        final PercentageStrategy strategy = new PercentageStrategy(
                Stream.concat(bins.stream().map(Segment::getPercent), Stream.of(0.0))
                      .collect(Collectors.toList()));
        
        final DefaultLimiter<Integer> limiter = new DefaultLimiter<>(limit, strategy);
        final int defaultBinIndex = bins.size();
        return new GrpcServerLimiter() {
            @Override
            public Optional<Limiter.Listener> acquire(ServerCall<?, ?> call, Metadata headers) {
                for (int i = 0; i < bins.size(); i++) {
                    if (bins.get(i).predicate.test(call, headers)) {
                        return limiter.acquire(i);
                    }
                }
                return limiter.acquire(defaultBinIndex);
            }
        };
    }

    private static class Segment {
        private final double percent;
        private final BiPredicate<ServerCall<?, ?>, Metadata> predicate;
        
        Segment(double percent, BiPredicate<ServerCall<?, ?>, Metadata> predicate) {
            this.percent = percent;
            this.predicate = predicate;
        }
        
        double getPercent() {
            return percent;
        }
    }
}

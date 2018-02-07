package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractLimiterBuilder;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.strategy.LookupPartitionStrategy;

import io.grpc.CallOptions;

import java.util.function.Consumer;

/**
 * Builder to simplify creating a {@link Limiter} specific to GRPC clients. 
 */
public final class GrpcClientLimiterBuilder extends AbstractLimiterBuilder<GrpcClientLimiterBuilder, GrpcClientRequestContext> {
    private boolean blockOnLimit = false;
    
    public GrpcClientLimiterBuilder partitionByMethod(Consumer<LookupPartitionStrategy.Builder<GrpcClientRequestContext>> configurer) {
        return partitionByLookup(context -> context.getMethod().getFullMethodName(), configurer);
    }
    
    public GrpcClientLimiterBuilder partitionByCallOption(CallOptions.Key<String> option, Consumer<LookupPartitionStrategy.Builder<GrpcClientRequestContext>> configurer) {
        return partitionByLookup(context -> context.getCallOptions().getOption(option), configurer);
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
    
    @Override
    protected GrpcClientLimiterBuilder self() {
        return this;
    }
    
    public Limiter<GrpcClientRequestContext> build() {
        Limiter<GrpcClientRequestContext> limiter = buildLimiter();
        if (blockOnLimit) {
            limiter = BlockingLimiter.wrap(limiter);
        }
        return limiter;
    }
}

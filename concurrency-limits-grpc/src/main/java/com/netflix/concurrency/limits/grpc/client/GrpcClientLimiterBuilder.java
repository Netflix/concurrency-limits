package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.grpc.CallOptions;

/**
 * Builder to simplify creating a {@link Limiter} specific to GRPC clients. 
 */
public final class GrpcClientLimiterBuilder extends AbstractPartitionedLimiter.Builder<GrpcClientLimiterBuilder, GrpcClientRequestContext> {
    private boolean blockOnLimit = false;

    public GrpcClientLimiterBuilder partitionByMethod() {
        return partitionResolver(context -> context.getMethod().getFullMethodName());
    }
    
    public GrpcClientLimiterBuilder partitionByCallOption(CallOptions.Key<String> option) {
        return partitionResolver(context -> context.getCallOptions().getOption(option));
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
        Limiter<GrpcClientRequestContext> limiter = super.build();

        if (blockOnLimit) {
            limiter = BlockingLimiter.wrap(limiter);
        }
        return limiter;
    }
}

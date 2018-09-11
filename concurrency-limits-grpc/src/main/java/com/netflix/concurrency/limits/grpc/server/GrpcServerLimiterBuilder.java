package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.grpc.Attributes;
import io.grpc.Metadata;

public class GrpcServerLimiterBuilder extends AbstractPartitionedLimiter.Builder<GrpcServerLimiterBuilder, GrpcServerRequestContext> {
    /**
     * Partition the limit by method
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByMethod() {
        return partitionResolver((GrpcServerRequestContext context) -> context.getCall().getMethodDescriptor().getFullMethodName());
    }

    /**
     * Partition the limit by a request header.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByHeader(Metadata.Key<String> header) {
        return partitionResolver(context -> context.getHeaders().get(header));
    }

    /**
     * Partition the limit by a request attribute.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByAttribute(Attributes.Key<String> attribute) {
        return partitionResolver(context -> context.getCall().getAttributes().get(attribute));
    }

    @Override
    protected GrpcServerLimiterBuilder self() {
        return this;
    }
}

package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractLimiterBuilder;
import com.netflix.concurrency.limits.strategy.LookupPartitionStrategy;

import io.grpc.Attributes;
import io.grpc.Metadata;

import java.util.function.Consumer;

/**
 * Builder to simplify creating a {@link Limiter} specific to GRPC server. By default,
 * the same concurrency limit is shared by all requests.  The limiter can be partitioned
 * based on one of many request attributes.  Only one type of partition may be specified.
 */
public final class GrpcServerLimiterBuilder extends AbstractLimiterBuilder<GrpcServerLimiterBuilder, GrpcServerRequestContext> {
    /**
     * Partition the limit by method
     * @param configurer Configuration function though which method percentages may be specified
     *                   Unspecified methods may only use excess capacity.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByMethod(Consumer<LookupPartitionStrategy.Builder<GrpcServerRequestContext>> configurer) {
        return partitionByLookup(context -> context.getCall().getMethodDescriptor().getFullMethodName(), configurer);
    }
    
    /**
     * Partition the limit by a request header. 
     * @param configurer Configuration function though which header value percentages may be specified.
     *                   Unspecified header values may only use excess capacity.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByHeader(Metadata.Key<String> header, Consumer<LookupPartitionStrategy.Builder<GrpcServerRequestContext>> configurer) {
        return partitionByLookup(context -> context.getHeaders().get(header), configurer);
    }
    
    /**
     * Partition the limit by a request attribute. 
     * @param configurer Configuration function though which attribute value percentages may be specified.
     *                   Unspecified attribute values may only use excess capacity.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByAttribute(Attributes.Key<String> attribute, Consumer<LookupPartitionStrategy.Builder<GrpcServerRequestContext>> configurer) {
        return partitionByLookup(context -> context.getCall().getAttributes().get(attribute), configurer);
    }
    
    @Override
    protected GrpcServerLimiterBuilder self() {
        return this;
    }
    
    public Limiter<GrpcServerRequestContext> build() {
        return buildLimiter();
    }
}

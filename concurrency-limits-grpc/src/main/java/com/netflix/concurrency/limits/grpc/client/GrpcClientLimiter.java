package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limiter;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

import java.util.Optional;

/**
 * Limiter contract specifically for GRPC clients.  
 * 
 * @see GrpcClientLimiterBuilder
 */
public interface GrpcClientLimiter {
    /**
     * Acquire a limit given a request context.
     * 
     * @param method
     * @param callOptions
     * @return Valid Listener if acquired or Optional.empty() if limit reached
     */
    Optional<Limiter.Listener> acquire(MethodDescriptor<?, ?> method, CallOptions callOptions);
}
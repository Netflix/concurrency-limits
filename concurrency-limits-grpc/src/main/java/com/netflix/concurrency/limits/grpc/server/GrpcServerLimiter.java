package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;

import io.grpc.Metadata;
import io.grpc.ServerCall;

import java.util.Optional;

/**
 * Limiter contract specifically for GRPC clients.  
 * 
 * @see GrpcServerLimiterBuilder
 */
public interface GrpcServerLimiter {
    /**
     * Acquire a limit given a request context.
     * 
     * @param method
     * @param callOptions
     * @return Valid Listener if acquired or Optional.empty() if limit reached
     */
    Optional<Limiter.Listener> acquire(ServerCall<?, ?> call, Metadata headers);
}
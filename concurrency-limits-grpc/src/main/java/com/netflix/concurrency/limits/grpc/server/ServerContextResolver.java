package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Contract for resolving a {@link Strategy}'s context to be used with segmented
 * limiters such as {@link PercentageStrategy}
 * 
 * @param <ContextT>
 */
public interface ServerContextResolver<ContextT> {
    /**
     * Determines the context from a request's callOptions and headers.
     * 
     * @param callOptions
     * @param headers
     * @return A valid context for the configured {@link Strategy}
     */
    ContextT resolve(MethodDescriptor<?,?> method, Metadata headers);
    
    static ServerContextResolver<Void> none() {
        return (method, headers) -> null;
    }
}
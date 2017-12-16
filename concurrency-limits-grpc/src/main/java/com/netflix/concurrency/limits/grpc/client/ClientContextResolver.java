package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;

import io.grpc.CallOptions;
import io.grpc.Metadata;

/**
 * Contract for resolving a {@link Strategy}'s context to be used with segmented
 * limiters such as {@link PercentageStrategy}
 * 
 * @param <ContextT>
 */
public interface ClientContextResolver<ContextT> {
    /**
     * Determines the context from a request's callOptions and headers.
     * 
     * @param callOptions
     * @param headers
     * @return A valid context for the configured {@link Strategy}
     */
    ContextT resolve(CallOptions callOptions, Metadata headers);
    
    static ClientContextResolver<Void> none() {
        return (options, headers) -> null;
    }
}
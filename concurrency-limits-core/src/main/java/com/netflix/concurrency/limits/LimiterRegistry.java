package com.netflix.concurrency.limits;

/**
 * {@link Limiter} lookup for integrations that support multiple Limiters, i.e. one per RPC method.
 * 
 * @param <ContextT>
 */
public interface LimiterRegistry<ContextT> {
    Limiter<ContextT> get(String key);
    
    static <ContextT> LimiterRegistry<ContextT> single(Limiter<ContextT> limiter) {
        return key -> limiter;
    }
}

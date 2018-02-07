package com.netflix.concurrency.limits;

import java.util.function.Supplier;

/**
 * Simple abstraction for tracking metrics in the limiters.
 * 
 */
public interface MetricRegistry {
    /**
     * Listener to receive samples for a distribution
     */
    interface SampleListener {
        void addSample(Number value);
    }
    
    /**
     * Register a sample distribution.  Samples are added to the distribution via the returned
     * {@link SampleListener}.  Will reuse an existing {@link SampleListener} if the distribution already
     * exists.
     * 
     * @param id
     * @param tagNameValuePairs Pairs of tag name and tag value.  Number of parameters must be a multiple of 2.
     * @return SampleListener for the caller to add samples
     */
    SampleListener registerDistribution(String id, String... tagNameValuePairs);
    
    /**
     * Register a gauge using the provided supplier.  The supplier will be polled whenever the guage
     * value is flushed by the registry.
     * 
     * @param id
     * @param tagNameValuePairs Pairs of tag name and tag value.  Number of parameters must be a multiple of 2.
     * @param supplier
     */
    void registerGauge(String id, Supplier<Number> supplier, String... tagNameValuePairs);
    
    /**
     * @deprecated Call MetricRegistry#registerGauge
     */
    @Deprecated
    default void registerGuage(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        registerGauge(id, supplier, tagNameValuePairs);
    }
}

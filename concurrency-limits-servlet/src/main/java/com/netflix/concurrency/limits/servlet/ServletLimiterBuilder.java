package com.netflix.concurrency.limits.servlet;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractLimiterBuilder;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.LookupPartitionStrategy;

import java.security.Principal;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.servlet.http.HttpServletRequest;

/**
 * Builder to simplify creating a {@link Limiter} specific to a Servlet filter. By default,
 * the same concurrency limit is shared by all requests.  The limiter can be partitioned
 * based on one of many request attributes.  Only one type of partition may be specified.
 */
public final class ServletLimiterBuilder extends AbstractLimiterBuilder<ServletLimiterBuilder, HttpServletRequest> {
    /**
     * Partition the limit by header
     * @param configurer Configuration function though which header percentages may be specified
     *                   Unspecified header values may only use excess capacity.
     * @return Chainable builder
     */
    public ServletLimiterBuilder partitionByHeader(String name, Consumer<LookupPartitionStrategy.Builder<HttpServletRequest>> configurer) {
        return partitionByLookup(
                request -> Optional.ofNullable(request.getHeader(name)).orElse(null),
                configurer);
    }
    
    /**
     * Partition the limit by {@link Principal}. Percentages of the limit are partitioned to named
     * groups.  Group membership is derived from the provided mapping function.
     * @param principalToGroup Mapping function from {@link Principal} to a named group.
     * @param configurer Configuration function though which group percentages may be specified
     *                   Unspecified group values may only use excess capacity.
     * @return Chainable builder
     */
    public ServletLimiterBuilder partitionByUserPrincipal(Function<Principal, String> principalToGroup, Consumer<LookupPartitionStrategy.Builder<HttpServletRequest>> configurer) {
        return partitionByLookup(
                request -> Optional.ofNullable(request.getUserPrincipal()).map(principalToGroup) .orElse(null),
                configurer);
    }
    
    /**
     * Partition the limit by request attribute
     * @param configurer Configuration function though which attribute percentages may be specified
     *                   Unspecified attribute values may only use excess capacity.
     * @return Chainable builder
     */
    public ServletLimiterBuilder partitionByAttribute(String name, Consumer<LookupPartitionStrategy.Builder<HttpServletRequest>> configurer) {
        return partitionByLookup(
                request -> Optional.ofNullable(request.getAttribute(name)).map(Object::toString).orElse(null),
                configurer);
    }
    
    /**
     * Partition the limit by request parameter
     * @param configurer Configuration function though which parameter value percentages may be specified
     *                   Unspecified parameter values may only use excess capacity.
     * @return Chainable builder
     */
    public ServletLimiterBuilder partitionByParameter(String name, Consumer<LookupPartitionStrategy.Builder<HttpServletRequest>> configurer) {
        return partitionByLookup(
                request -> Optional.ofNullable(request.getParameter(name)).orElse(null),
                configurer);
    }
    
    /**
     * Partition the limit by the full path. Percentages of the limit are partitioned to named
     * groups.  Group membership is derived from the provided mapping function.
     * @param pathToGroup Mapping function from full path to a named group.
     * @param configurer Configuration function though which group percentages may be specified
     *                   Unspecified group values may only use excess capacity.
     * @return Chainable builder
     */
    public ServletLimiterBuilder partitionByPathInfo(Function<String, String> pathToGroup, Consumer<LookupPartitionStrategy.Builder<HttpServletRequest>> configurer) {
        return partitionByLookup(
                request -> Optional.ofNullable(request.getPathInfo()).orElse(null),
                configurer);
    }
    
    @Override
    protected ServletLimiterBuilder self() {
        return this;
    }
    
    public Limiter<HttpServletRequest> build() {
        return DefaultLimiter.newBuilder().limit(limit).build(getFinalStrategy());
    }
}

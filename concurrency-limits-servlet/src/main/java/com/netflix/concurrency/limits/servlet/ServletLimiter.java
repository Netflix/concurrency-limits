package com.netflix.concurrency.limits.servlet;

import com.netflix.concurrency.limits.Limiter;

import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

/**
 * Limiter contract specifically for Servlet Filters.  
 * 
 * @see ConcurrencyLimitServletFilter
 */
public interface ServletLimiter {
    /**
     * Acquire a limit given a request context.
     * 
     * @param request The incoming HttpServletRequest
     * @return Valid Listener if acquired or Optional.empty() if limit reached
     */
    Optional<Limiter.Listener> acquire(HttpServletRequest request);
}
package com.netflix.concurrency.limits.servlet;

import com.netflix.concurrency.limits.Limiter;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet {@link Filter} that enforces concurrency limits on all requests into the servlet.
 * 
 * @see GroupServletLimiter
 */
public class ConcurrencyLimitServletFilter implements Filter {

    private final ServletLimiter limiter;

    public ConcurrencyLimitServletFilter(ServletLimiter limiter) {
        this.limiter = limiter;
    }
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        Optional<Limiter.Listener> listener = limiter.acquire((HttpServletRequest)request);
        if (listener.isPresent()) {
            try {
                chain.doFilter(request, response);
                listener.get().onSuccess();
            } catch (Exception e) {
                listener.get().onIgnore();
            }
        } else {
            outputThrottleError((HttpServletResponse)response);
        }
    }

    protected void outputThrottleError(HttpServletResponse response) {
        try {
            response.setStatus(429);
            response.getWriter().print("Concurrency limit exceeded");
        } catch (IOException e) {
        }
    }
    
    @Override
    public void destroy() {
    }
}

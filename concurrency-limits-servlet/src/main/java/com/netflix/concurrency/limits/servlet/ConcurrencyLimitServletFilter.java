/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * @see ServletLimiterBuilder
 */
public class ConcurrencyLimitServletFilter implements Filter {

    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private final Limiter<HttpServletRequest> limiter;

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter) {
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
            response.setStatus(STATUS_TOO_MANY_REQUESTS);
            response.getWriter().print("Concurrency limit exceeded");
        } catch (IOException e) {
        }
    }
    
    @Override
    public void destroy() {
    }
}

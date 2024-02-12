/**
 * Copyright 2023 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.servlet.jakarta;

import com.netflix.concurrency.limits.Limiter;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Servlet {@link Filter} that enforces concurrency limits on all requests into the servlet.
 *
 * @see ServletLimiterBuilder
 */
public class ConcurrencyLimitServletFilter implements Filter {
    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private final Limiter<HttpServletRequest> limiter;
    private final int throttleStatus;
    private final Predicate<HttpServletRequest> bypassFilterPredicate;

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter) {
        this(limiter, STATUS_TOO_MANY_REQUESTS, httpRequest -> false);
    }

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter, int throttleStatus) {
        this(limiter, throttleStatus, httpRequest -> false);
    }

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter, Predicate<HttpServletRequest> bypassFilterPredicate) {
        this(limiter, STATUS_TOO_MANY_REQUESTS, bypassFilterPredicate);
    }

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter, int throttleStatus, Predicate<HttpServletRequest> bypassFilterPredicate) {
        this.limiter = limiter;
        this.throttleStatus = throttleStatus;
        this.bypassFilterPredicate = bypassFilterPredicate;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;

        // Bypass filter logic and don't acquire limiter token if predicate condition is met
        if (bypassFilterPredicate.test(httpRequest)) {
            chain.doFilter(request, response);
            return;
        }

        Optional<Limiter.Listener> listener = limiter.acquire(httpRequest);
        if (listener.isPresent()) {
            try {
                chain.doFilter(request, response);
                listener.get().onSuccess();
            } catch (Exception e) {
                listener.get().onIgnore();
                throw e;
            }
        } else {
            outputThrottleError((HttpServletResponse) response);
        }
    }

    protected void outputThrottleError(HttpServletResponse response) {
        try {
            response.setStatus(throttleStatus);
            response.getWriter().print("Concurrency limit exceeded");
        } catch (IOException e) {
        }
    }

    @Override
    public void destroy() {
    }
}

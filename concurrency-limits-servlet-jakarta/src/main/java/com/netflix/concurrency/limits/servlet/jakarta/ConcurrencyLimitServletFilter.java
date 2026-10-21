/**
 * Copyright 2023 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.servlet.jakarta;

import com.netflix.concurrency.limits.Limiter;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Servlet {@link Filter} that enforces concurrency limits on all requests into the servlet.
 *
 * @see ServletLimiterBuilder
 */
public class ConcurrencyLimitServletFilter implements Filter {

    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private static final AtomicLong NEXT_FILTER_ID = new AtomicLong();
    private final Limiter<HttpServletRequest> limiter;
    private final int throttleStatus;
    private final String listenerAttribute = ConcurrencyLimitServletFilter.class.getName()
            + "." + NEXT_FILTER_ID.getAndIncrement();

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter) {
        this(limiter, STATUS_TOO_MANY_REQUESTS);
    }

    public ConcurrencyLimitServletFilter(Limiter<HttpServletRequest> limiter, int throttleStatus) {
        this.limiter = limiter;
        this.throttleStatus = throttleStatus;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        Completion existing = (Completion) request.getAttribute(listenerAttribute);
        if (existing != null) {
            try {
                chain.doFilter(request, response);
            } catch (IOException | ServletException | RuntimeException | Error e) {
                existing.failed = true;
                throw e;
            }
            return;
        }
        Optional<Limiter.Listener> listener = limiter.acquire((HttpServletRequest) request);
        if (listener.isPresent()) {
            Completion completion = new Completion((HttpServletRequest) request, listener.get());
            request.setAttribute(listenerAttribute, completion);
            try {
                if (request.isAsyncStarted()) {
                    completion.observe(request.getAsyncContext());
                }
                chain.doFilter(completion, response);
            } catch (IOException | ServletException | RuntimeException | Error e) {
                completion.failed = true;
                throw e;
            } finally {
                if (!completion.asyncStarted) {
                    completion.complete();
                }
            }
        } else {
            outputThrottleError((HttpServletResponse) response);
            if (request.isAsyncStarted()) {
                request.getAsyncContext().complete();
            }
        }
    }

    private static final class Completion extends HttpServletRequestWrapper implements AsyncListener {
        private final Limiter.Listener listener;
        private final AtomicBoolean completed = new AtomicBoolean();
        private boolean asyncStarted;
        private volatile boolean failed;
        private volatile boolean timedOut;

        private Completion(HttpServletRequest request, Limiter.Listener listener) {
            super(request);
            this.listener = listener;
        }

        @Override
        public AsyncContext startAsync() {
            return observe(super.startAsync());
        }

        @Override
        public AsyncContext startAsync(ServletRequest request, ServletResponse response) {
            return observe(super.startAsync(request, response));
        }

        private AsyncContext observe(AsyncContext context) {
            // Attach before the application can complete or dispatch the context.
            if (!asyncStarted) {
                context.addListener(this);
                asyncStarted = true;
            }
            return context;
        }

        @Override
        public void onStartAsync(AsyncEvent event) {
            event.getAsyncContext().addListener(this);
        }

        @Override
        public void onComplete(AsyncEvent event) {
            complete();
        }

        @Override
        public void onTimeout(AsyncEvent event) {
            timedOut = true;
        }

        @Override
        public void onError(AsyncEvent event) {
            failed = true;
        }

        private void complete() {
            if (completed.compareAndSet(false, true)) {
                if (timedOut) {
                    listener.onDropped();
                } else if (failed) {
                    listener.onIgnore();
                } else {
                    listener.onSuccess();
                }
            }
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

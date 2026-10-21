/**
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.concurrency.limits;

import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.servlet.ConcurrencyLimitServletFilter;
import com.netflix.concurrency.limits.servlet.ServletLimiterBuilder;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class ConcurrencyLimitServletFilterAsyncTest {
    @Parameterized.Parameters(name = "async dispatcher mapping: {0}")
    public static Object[][] mappings() {
        return new Object[][] {{false}, {true}};
    }

    @Parameterized.Parameter
    public boolean mapAsyncDispatch;

    private final BlockingQueue<AsyncContext> pending = new LinkedBlockingQueue<>();
    private final BlockingQueue<DispatcherType> returned = new LinkedBlockingQueue<>();
    private final AtomicInteger successes = new AtomicInteger();
    private final AtomicInteger ignored = new AtomicInteger();
    private final AtomicInteger dropped = new AtomicInteger();
    private final AtomicInteger secondarySuccesses = new AtomicInteger();
    private final Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
            .limit(FixedLimit.of(1))
            .bypassLimitByPathInfo("/health")
            .bypassLimitByPathInfo("/secondary/only")
            .build();
    private final Limiter<HttpServletRequest> secondaryLimiter = new ServletLimiterBuilder()
            .limit(FixedLimit.of(1))
            .build();
    private final Limiter<HttpServletRequest> observed = request -> limiter.acquire(request).map(listener -> new Limiter.Listener() {
        @Override
        public void onSuccess() {
            listener.onSuccess();
            successes.incrementAndGet();
        }

        @Override
        public void onIgnore() {
            listener.onIgnore();
            ignored.incrementAndGet();
        }

        @Override
        public void onDropped() {
            listener.onDropped();
            dropped.incrementAndGet();
        }
    });

    @Rule
    public final HttpServerRule server = new HttpServerRule(context -> {
        FilterHolder earlier = new FilterHolder(new Filter() {
            @Override
            public void init(FilterConfig config) {
            }

            @Override
            public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                    throws IOException, ServletException {
                if (request.getDispatcherType() == DispatcherType.REQUEST
                        && ((HttpServletRequest) request).getPathInfo().startsWith("/already-async")) {
                    request.startAsync().setTimeout(0);
                }
                try {
                    chain.doFilter(request, response);
                } finally {
                    returned.add(request.getDispatcherType());
                }
            }

            @Override
            public void destroy() {
            }
        });
        earlier.setAsyncSupported(true);
        context.addFilter(earlier, "/*", mapAsyncDispatch
                ? EnumSet.allOf(DispatcherType.class) : EnumSet.of(DispatcherType.REQUEST));
        FilterHolder filter = new FilterHolder(new ConcurrencyLimitServletFilter(observed));
        filter.setAsyncSupported(true);
        context.addFilter(filter, "/*", mapAsyncDispatch
                ? EnumSet.allOf(DispatcherType.class) : EnumSet.of(DispatcherType.REQUEST));
        FilterHolder secondary = new FilterHolder(new ConcurrencyLimitServletFilter(
                request -> secondaryLimiter.acquire(request).map(listener -> new Limiter.Listener() {
                    @Override
                    public void onSuccess() {
                        listener.onSuccess();
                        secondarySuccesses.incrementAndGet();
                    }

                    @Override
                    public void onIgnore() {
                        listener.onIgnore();
                    }

                    @Override
                    public void onDropped() {
                        listener.onDropped();
                    }
                })));
        secondary.setAsyncSupported(true);
        context.addFilter(secondary, "/secondary/*", mapAsyncDispatch
                ? EnumSet.allOf(DispatcherType.class) : EnumSet.of(DispatcherType.REQUEST));
        ServletHolder servlet = new ServletHolder(new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException {
                String path = request.getPathInfo();
                if ("/forward-sync".equals(path) || "/forward-async".equals(path)) {
                    request.getRequestDispatcher("/forward-sync".equals(path) ? "/sync" : "/async")
                            .forward(request, response);
                    return;
                }
                if ("/throw".equals(path)) {
                    throw new ServletException("expected");
                }
                if ("/fatal".equals(path)) {
                    throw new AssertionError("expected");
                }
                if ("/send-error".equals(path)) {
                    response.sendError(500);
                    return;
                }
                if ("/sync".equals(path) || "/secondary/sync".equals(path) || "/health".equals(path)
                        || (request.getDispatcherType() == DispatcherType.ASYNC && !path.endsWith("/restart"))) {
                    response.getWriter().write("done");
                    return;
                }
                AsyncContext async = request.isAsyncStarted() ? request.getAsyncContext()
                        : "/explicit".equals(path) ? request.startAsync(request, response) : request.startAsync();
                async.setTimeout(path.contains("timeout") ? 100 : 0);
                if ("/timeout-recover".equals(path)) {
                    async.addListener(new AsyncListener() {
                        @Override
                        public void onTimeout(AsyncEvent event) {
                            event.getAsyncContext().dispatch("/restart");
                        }

                        @Override
                        public void onComplete(AsyncEvent event) {
                        }

                        @Override
                        public void onError(AsyncEvent event) {
                        }

                        @Override
                        public void onStartAsync(AsyncEvent event) {
                        }
                    });
                }
                if ("/immediate".equals(path)) {
                    async.complete();
                } else {
                    pending.add(async);
                }
            }
        });
        servlet.setAsyncSupported(true);
        context.addServlet(servlet, "/*");
    });

    @Test
    public void holdsCapacityUntilAsyncCompletion() throws Exception {
        assertAsyncCompletion("/async");
    }

    @Test
    public void alreadyAsyncRequestRetainsCapacity() throws Exception {
        assertRetainsCapacity("/already-async");
    }

    @Test
    public void alreadyAsyncRejectionCompletesHttpResponse() throws Exception {
        CompletableFuture<Integer> first = getAsync("/async");
        AsyncContext async = takePending(first);
        take(returned);
        try {
            assertEquals(429, get("/already-async"));
            assertFalse(first.isDone());
            assertEquals(0, successes.get());
        } finally {
            async.complete();
        }
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    @Test
    public void alreadyAsyncTimeoutRecordsDroppedOutcome() throws Exception {
        CompletableFuture<Integer> first = getAsync("/already-async-timeout");
        takePending(first);
        assertEquals(500, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(0, 0, 1);
        assertEquals(200, get("/sync"));
        awaitCounts(1, 0, 1);
    }

    @Test
    public void synchronousForwardReleasesExactlyOnce() throws Exception {
        assertEquals(200, get("/forward-sync"));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    @Test
    public void asynchronousForwardRetainsCapacity() throws Exception {
        assertRetainsCapacity("/forward-async");
    }

    @Test
    public void multipleFiltersRetainAndReleaseTheirOwnPermits() throws Exception {
        CompletableFuture<Integer> first = getAsync("/secondary/async");
        AsyncContext async = takePending(first);
        take(returned);
        try {
            assertFalse(first.isDone());
            assertEquals(429, get("/sync"));
            assertEquals(429, get("/secondary/only"));
            awaitCounts(1, 0); // The first filter bypasses /secondary/only.
            assertEquals(0, secondarySuccesses.get());
        } finally {
            async.complete();
        }
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(2, 0);
        awaitSecondarySuccesses(1);
        assertEquals(200, get("/secondary/sync"));
        awaitCounts(3, 0);
        awaitSecondarySuccesses(2);
    }

    @Test
    public void rejectionBySecondFilterRestoresFirstFilterCapacity() throws Exception {
        CompletableFuture<Integer> first = getAsync("/secondary/only");
        AsyncContext async = takePending(first);
        take(returned);
        try {
            assertEquals(429, get("/secondary/async"));
            awaitCounts(1, 0);
            assertEquals(200, get("/sync"));
            awaitCounts(2, 0);
            assertFalse(first.isDone());
            assertEquals(0, secondarySuccesses.get());
        } finally {
            async.complete();
        }
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(3, 0);
        awaitSecondarySuccesses(1);
        assertEquals(200, get("/secondary/sync"));
        awaitCounts(4, 0);
        awaitSecondarySuccesses(2);
    }

    @Test
    public void multipleFiltersRetainCapacityAcrossAnotherAsyncCycle() throws Exception {
        CompletableFuture<Integer> first = getAsync("/secondary/restart");
        AsyncContext initial = takePending(first);
        take(returned);
        initial.dispatch();
        AsyncContext next = takePending(first);
        if (mapAsyncDispatch) {
            take(returned);
        }
        try {
            assertFalse(first.isDone());
            assertEquals(0, successes.get());
            assertEquals(0, secondarySuccesses.get());
            assertEquals(429, get("/secondary/sync"));
        } finally {
            next.complete();
        }
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(1, 0);
        awaitSecondarySuccesses(1);
        assertEquals(200, get("/secondary/sync"));
        awaitCounts(2, 0);
        awaitSecondarySuccesses(2);
    }

    private void awaitSecondarySuccesses(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (secondarySuccesses.get() != count && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals("second filter success callbacks", count, secondarySuccesses.get());
    }

    private void assertRetainsCapacity(String path) throws Exception {
        CompletableFuture<Integer> first = getAsync(path);
        AsyncContext async = takePending(first);
        try {
            while (take(returned) != DispatcherType.REQUEST) {
                // A nested forward returns before its enclosing REQUEST dispatch.
            }
            assertFalse(first.isDone());
            assertEquals(429, get("/sync"));
        } finally {
            async.complete();
        }
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    @Test
    public void supportsExplicitStartAsyncArguments() throws Exception {
        assertAsyncCompletion("/explicit");
    }

    private void assertAsyncCompletion(String path) throws Exception {
        CompletableFuture<Integer> first = getAsync(path);
        AsyncContext async = takePending(first);
        take(returned);
        assertFalse(first.isDone());
        assertEquals(0, successes.get());
        assertEquals(429, get("/sync"));
        assertEquals(200, get("/health"));
        async.getResponse().getWriter().write("done");
        async.complete();
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(2, 0); // Includes the bypassed health request.
        assertEquals(200, get("/sync"));
        awaitCounts(3, 0);
    }

    @Test
    public void asyncRedispatchDoesNotAcquireAgain() throws Exception {
        CompletableFuture<Integer> first = getAsync("/async");
        AsyncContext async = takePending(first);
        take(returned);
        async.dispatch();
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    @Test
    public void retainsCapacityAcrossAnotherAsyncCycle() throws Exception {
        CompletableFuture<Integer> first = getAsync("/restart");
        AsyncContext initial = takePending(first);
        take(returned);
        initial.dispatch();
        AsyncContext next = takePending(first);
        if (mapAsyncDispatch) {
            take(returned);
        }
        assertFalse(first.isDone());
        assertEquals(0, successes.get());
        assertEquals(429, get("/sync"));
        next.complete();
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    @Test
    public void immediateAsyncCompletionReleasesExactlyOnce() throws Exception {
        for (int i = 1; i <= 20; i++) {
            assertEquals(200, get("/immediate"));
            awaitCounts(i, 0);
        }
    }

    @Test
    public void timeoutReleasesOnHttpCompletion() throws Exception {
        CompletableFuture<Integer> first = getAsync("/timeout");
        takePending(first);
        assertEquals(500, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(0, 0, 1);
        assertEquals(200, get("/sync"));
        awaitCounts(1, 0, 1);
    }

    @Test
    public void timeoutRecoveryRetainsCapacityUntilCompletion() throws Exception {
        CompletableFuture<Integer> first = getAsync("/timeout-recover");
        takePending(first);
        take(returned);
        AsyncContext recovery = takePending(first);
        if (mapAsyncDispatch) {
            take(returned);
        }
        assertFalse(first.isDone());
        assertEquals(0, dropped.get());
        assertEquals(429, get("/sync"));
        recovery.complete();
        assertEquals(200, (int) first.get(5, TimeUnit.SECONDS));
        awaitCounts(0, 0, 1);
        assertEquals(200, get("/sync"));
        awaitCounts(1, 0, 1);
    }

    @Test
    public void disconnectedResponseReleasesOnHttpCompletion() throws Exception {
        Socket socket = new Socket("127.0.0.1", server.getPort());
        try {
            socket.getOutputStream().write(
                    "GET /async HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            AsyncContext async = take(pending);
            take(returned);
            socket.setSoLinger(true, 0);
            socket.close();
            assertEquals(429, get("/sync"));
            try {
                async.getResponse().getOutputStream().write(new byte[1024 * 1024]);
                async.getResponse().flushBuffer();
            } catch (IOException expected) {
                // A reset is observable when the application next writes the response.
            } finally {
                async.complete();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (successes.get() + ignored.get() != 1 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, successes.get() + ignored.get());
            assertEquals(200, get("/sync"));
        } finally {
            socket.close();
        }
    }

    @Test
    public void asyncFailureReleasesExactlyOnce() throws Exception {
        CompletableFuture<Integer> first = getAsync("/async");
        AsyncContext async = takePending(first);
        take(returned);
        async.dispatch("/throw");
        assertEquals(500, (int) first.get(5, TimeUnit.SECONDS));
        // A redispatch exception reaches this filter only with an ASYNC mapping.
        awaitCounts(mapAsyncDispatch ? 0 : 1, mapAsyncDispatch ? 1 : 0);
        assertEquals(200, get("/sync"));
        awaitCounts(mapAsyncDispatch ? 1 : 2, mapAsyncDispatch ? 1 : 0);
    }

    @Test
    public void synchronousFailureReleasesCapacity() throws Exception {
        assertEquals(500, get("/throw"));
        awaitCounts(0, 1);
        assertEquals(200, get("/sync"));
        awaitCounts(1, 1);
    }

    @Test
    public void synchronousErrorReleasesCapacity() throws Exception {
        assertEquals(500, get("/fatal"));
        awaitCounts(0, 1);
        assertEquals(200, get("/sync"));
        awaitCounts(1, 1);
    }

    @Test
    public void sendErrorReleasesExactlyOnce() throws Exception {
        assertEquals(500, get("/send-error"));
        awaitCounts(1, 0);
        assertEquals(200, get("/sync"));
        awaitCounts(2, 0);
    }

    private CompletableFuture<Integer> getAsync(String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return get(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int get(String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection)
                new URL("http://127.0.0.1:" + server.getPort() + path).openConnection(Proxy.NO_PROXY);
        connection.setRequestProperty("Connection", "close");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    private AsyncContext takePending(CompletableFuture<Integer> response) throws InterruptedException {
        AsyncContext value = pending.poll(5, TimeUnit.SECONDS);
        assertNotNull("container event; HTTP response: " + response.getNow(null), value);
        return value;
    }

    private static <T> T take(BlockingQueue<T> queue) throws InterruptedException {
        T value = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull("container event", value);
        return value;
    }

    private void awaitCounts(int success, int failure) throws InterruptedException {
        awaitCounts(success, failure, 0);
    }

    private void awaitCounts(int success, int failure, int timeout) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((successes.get() != success || ignored.get() != failure || dropped.get() != timeout)
                && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals("success callbacks", success, successes.get());
        assertEquals("ignore callbacks", failure, ignored.get());
        assertEquals("dropped callbacks", timeout, dropped.get());
    }
}

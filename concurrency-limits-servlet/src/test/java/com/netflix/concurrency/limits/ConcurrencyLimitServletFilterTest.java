package com.netflix.concurrency.limits;

import com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.servlet.ConcurrencyLimitServletFilter;
import com.netflix.concurrency.limits.servlet.ServletLimiterBuilder;

import java.io.IOException;
import java.security.Principal;
import java.util.EnumSet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.FilterHolder;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class ConcurrencyLimitServletFilterTest {
    @ClassRule
    public static HttpServerRule server = new HttpServerRule(context -> {
        context.addServlet(HelloServlet.class, "/");
        
        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(FixedLimit.of(10))
                .partitionByUserPrincipal(Principal::getName)
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        FilterHolder holder = new FilterHolder();
        holder.setFilter(new ConcurrencyLimitServletFilter(limiter));
        
        context.addFilter(holder, "/*", EnumSet.of(DispatcherType.REQUEST));
    });
    
    @Test
    @Ignore
    public void simulation() throws Exception {
        Limit limit = VegasLimit.newDefault();
        BlockingAdaptiveExecutor executor = new BlockingAdaptiveExecutor(
                SimpleLimiter.newBuilder().limit(limit).build());
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(String.format("errors=%d success=%d limit=%s", errors.getAndSet(0), success.getAndSet(0), limit));
        }, 1, 1, TimeUnit.SECONDS);
        
        
        while (true) {
            executor.execute(() -> {
                try {
                    server.get("/batch");
                    success.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                    throw new RejectedExecutionException();
                }
            });
        }
    }
    
    public static class HelloServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
            }
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("Hello from HelloServlet");
        }
    }
}

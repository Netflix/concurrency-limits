package com.netflix.concurrency.limits;

import com.netflix.concurrency.limits.servlet.ConcurrencyLimitServletFilter;
import com.netflix.concurrency.limits.servlet.ServletLimiterBuilder;
import com.netflix.concurrency.limits.spectator.SpectatorMetricRegistry;
import com.netflix.spectator.api.DefaultRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class ConcurrencyLimitServletFilterTest {

    @Rule
    public TestName testName = new TestName();

    Limiter<HttpServletRequest> limiter;
    DefaultRegistry registry = new DefaultRegistry();
    SpectatorMetricRegistry spectatorMetricRegistry = new SpectatorMetricRegistry(registry, registry.createId("unit.test.limiter"));

    @Before
    public void beforeEachTest() {

        // Will bypass GET calls or calls with /admin path or both
        limiter = Mockito.spy(new ServletLimiterBuilder()
                .bypassLimitByMethod("GET")
                .bypassLimitByPathInfo("/admin/health")
                .named(testName.getMethodName())
                .metricRegistry(spectatorMetricRegistry)
                .build());
    }

    @Test
    public void testDoFilterAllowed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);
        assertEquals("Request should be passed to the downstream chain", request, filterChain.getRequest());
        assertEquals("Response should be passed to the downstream chain", response, filterChain.getResponse());

        verifyCounts(0, 0, 1, 0, 0);
    }

    @Test
    public void testDoFilterThrottled() throws ServletException, IOException {
        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        //Empty means to throttle this request
        doReturn(Optional.empty()).when(limiter).acquire(any());

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(new MockHttpServletRequest(), response, filterChain);

        assertNull("doFilter should not be called on the filterchain", filterChain.getRequest());
        assertEquals("Status should be 429 - too many requests", 429, response.getStatus());
    }

    @Test
    public void testDoFilterThrottledCustomStatus() throws ServletException, IOException {
        final int customThrottleStatus = 503;
        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter, customThrottleStatus);

        //Empty means to throttle this request
        doReturn(Optional.empty()).when(limiter).acquire(any());

        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(new MockHttpServletRequest(), response, new MockFilterChain());

        assertEquals("custom status should be respected", customThrottleStatus, response.getStatus());

    }

    @Test
    public void testDoFilterBypassCheckPassedForMethod() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("GET");
        request.setPathInfo("/live/path");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);

        assertEquals("Request should be passed to the downstream chain", request, filterChain.getRequest());
        assertEquals("Response should be passed to the downstream chain", response, filterChain.getResponse());
        verifyCounts(0, 0, 0, 0, 1);

    }

    @Test
    public void testDoFilterBypassCheckPassedForPath() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("POST");
        request.setPathInfo("/admin/health");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);

        assertEquals("Request should be passed to the downstream chain", request, filterChain.getRequest());
        assertEquals("Response should be passed to the downstream chain", response, filterChain.getResponse());
        verifyCounts(0, 0, 0, 0, 1);
    }

    @Test
    public void testDoFilterBypassCheckFailed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("POST");
        request.setPathInfo("/live/path");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);

        assertEquals("Request should be passed to the downstream chain", request, filterChain.getRequest());
        assertEquals("Response should be passed to the downstream chain", response, filterChain.getResponse());
        verifyCounts(0, 0, 1, 0, 0);
    }

    public void verifyCounts(int dropped, int ignored, int success, int rejected, int bypassed) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        assertEquals(dropped, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "dropped").count());
        assertEquals(ignored, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "ignored").count());
        assertEquals(success, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "success").count());
        assertEquals(rejected, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "rejected").count());
        assertEquals(bypassed, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "bypassed").count());
    }
}

package com.netflix.concurrency.limits;


import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.servlet.ConcurrencyLimitServletFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ConcurrencyLimitServletFilterTest {

    Predicate<HttpServletRequest> bypassActuatorEndpointPredicate = (request) ->
            request.getRequestURI().contains("/admin");
    @Spy
    Limiter<HttpServletRequest> limiter = SimpleLimiter.<HttpServletRequest>newBypassLimiterBuilder()
            .shouldBypass(bypassActuatorEndpointPredicate)
            .build();

    @Mock
    Limiter.Listener listener;

    @Test
    public void testDoFilterAllowed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        doReturn(Optional.of(listener)).when(limiter).acquire(any());

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();


        filter.doFilter(request, response, filterChain);
        assertEquals(request, filterChain.getRequest(), "Request should be passed to the downstream chain");
        assertEquals(response, filterChain.getResponse(), "Response should be passed to the downstream chain");

        verify(listener).onSuccess();
    }

    @Test
    public void testDoFilterThrottled() throws ServletException, IOException {
        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        //Empty means to throttle this request
        doReturn(Optional.empty()).when(limiter).acquire(any());

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();


        filter.doFilter(new MockHttpServletRequest(), response, filterChain);

        assertNull(filterChain.getRequest(), "doFilter should not be called on the filterchain");

        assertEquals(429, response.getStatus(), "Status should be 429 - too many requests");
    }

    @Test
    public void testDoFilterThrottledCustomStatus() throws ServletException, IOException {
        final int customThrottleStatus = 503;
        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter, customThrottleStatus);

        //Empty means to throttle this request
        doReturn(Optional.empty()).when(limiter).acquire(any());

        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(new MockHttpServletRequest(), response,  new MockFilterChain());

        assertEquals(customThrottleStatus, response.getStatus(), "custom status should be respected");
    }

    @Test
    public void testDoFilterBypassCheckPassed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/admin/health");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);

        assertEquals(request, filterChain.getRequest(), "Request should be passed to the downstream chain");
        assertEquals(response, filterChain.getResponse(), "Response should be passed to the downstream chain");

        // Acquire a bypass listener
        verify(limiter, times(1)).acquire(isA(HttpServletRequest.class));
    }

    @Test
    public void testDoFilterBypassCheckFailed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/live/path");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        filter.doFilter(request, response, filterChain);

        assertEquals(request, filterChain.getRequest(), "Request should be passed to the downstream chain");
        assertEquals(response, filterChain.getResponse(), "Response should be passed to the downstream chain");

        // Acquire a non bypass listener
        verify(limiter, times(1)).acquire(isA(HttpServletRequest.class));
    }
}

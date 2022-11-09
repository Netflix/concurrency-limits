package com.netflix.concurrency.limits;


import com.netflix.concurrency.limits.servlet.ConcurrencyLimitServletFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConcurrencyLimitServletFilterTest {

    @Mock
    Limiter<HttpServletRequest> limiter;

    @Mock
    Limiter.Listener listener;

    @Test
    public void testDoFilterAllowed() throws ServletException, IOException {

        ConcurrencyLimitServletFilter filter = new ConcurrencyLimitServletFilter(limiter);

        when(limiter.acquire(any())).thenReturn(Optional.of(listener));

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
        when(limiter.acquire(any())).thenReturn(Optional.empty());

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
        when(limiter.acquire(any())).thenReturn(Optional.empty());

        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(new MockHttpServletRequest(), response,  new MockFilterChain());

        assertEquals(customThrottleStatus, response.getStatus(), "custom status should be respected");
    }
}

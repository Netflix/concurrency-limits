package com.netflix.concurrency.limits;

import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.servlet.ServletLimiterBuilder;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupServletLimiterTest {

    @Test
    public void userPrincipalMatchesGroup() {
        Map<String, String> principalToGroup = Mockito.spy(new HashMap<>());
        principalToGroup.put("bob", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByUserPrincipal(p -> principalToGroup.get(p.getName()))
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPrincipal("bob");
        Optional<Listener> listener = limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(1)).get("bob");
    }

    @Test
    public void userPrincipalDoesNotMatchGroup() {
        Map<String, String> principalToGroup = Mockito.spy(new HashMap<>());
        principalToGroup.put("bob", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByUserPrincipal(p -> principalToGroup.get(p.getName()))
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPrincipal("doesntexist");
        Optional<Listener> listener = limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(1)).get("doesntexist");
    }

    @Test
    public void nullUserPrincipalDoesNotMatchGroup() {
        Map<String, String> principalToGroup = Mockito.spy(new HashMap<>());
        principalToGroup.put("bob", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByUserPrincipal(p -> principalToGroup.get(p.getName()))
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getUserPrincipal()).thenReturn(null);

        Optional<Listener> listener =  limiter.acquire(request);

        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(0)).get(Mockito.<String> any());
    }

    @Test
    public void nullUserPrincipalNameDoesNotMatchGroup() {
        Map<String, String> principalToGroup = Mockito.spy(new HashMap<>());
        principalToGroup.put("bob", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByUserPrincipal(p -> principalToGroup.get(p.getName()))
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPrincipal(null);
        Optional<Listener> listener =  limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(1)).get(ArgumentMatchers.isNull());
    }

    @Test
    public void pathMatchesGroup() {
        Map<String, String> pathToGroup = Mockito.spy(new HashMap<>());
        pathToGroup.put("/live/path", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByPathInfo(pathToGroup::get)
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPathInfo("/live/path");
        Optional<Listener> listener = limiter.acquire(request);

        Assert.assertTrue(listener.isPresent());
        Mockito.verify(pathToGroup, Mockito.times(1)).get("/live/path");
    }

    @Test
    public void pathDoesNotMatchesGroup() {
        Map<String, String> pathToGroup = Mockito.spy(new HashMap<>());
        pathToGroup.put("/live/path", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByPathInfo(pathToGroup::get)
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPathInfo("/other/path");
        Optional<Listener> listener = limiter.acquire(request);

        Assert.assertTrue(listener.isPresent());
        Mockito.verify(pathToGroup, Mockito.times(1)).get("/other/path");
    }

    @Test
    public void nullPathDoesNotMatchesGroup() {
        Map<String, String> pathToGroup = Mockito.spy(new HashMap<>());
        pathToGroup.put("/live/path", "live");

        Limiter<HttpServletRequest> limiter = new ServletLimiterBuilder()
                .limit(VegasLimit.newDefault())
                .partitionByPathInfo(pathToGroup::get)
                .partition("live", 0.8)
                .partition("batch", 0.2)
                .build();

        HttpServletRequest request = createMockRequestWithPathInfo(null);
        Optional<Listener> listener = limiter.acquire(request);

        Assert.assertTrue(listener.isPresent());
        Mockito.verify(pathToGroup, Mockito.times(0)).get(Mockito.<String> any());
    }

    private HttpServletRequest createMockRequestWithPrincipal(String name) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Principal principal = Mockito.mock(Principal.class);

        Mockito.when(request.getUserPrincipal()).thenReturn(principal);
        Mockito.when(principal.getName()).thenReturn(name);
        return request;
    }

    private HttpServletRequest createMockRequestWithPathInfo(String name) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(request.getPathInfo()).thenReturn(name);
        return request;
    }
}

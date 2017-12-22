package com.netflix.concurrency.limits;

import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.servlet.GroupServletLimiter;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.mockito.Mockito;

import junit.framework.Assert;

public class GroupServletLimiterTest {
    private static final Map<String, Double> groupToPercent = new HashMap<>();
    private static final Map<String, String> principalToGroup = Mockito.spy(new HashMap<>());
    
    static {
        principalToGroup.put("bob", "live");
        
        groupToPercent.put("live", 0.8);
        groupToPercent.put("batch", 0.2);
    }
    
    HttpServletRequest createMockRequestWithPrincipal(String name) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Principal principal = Mockito.mock(Principal.class);
        
        Mockito.when(request.getUserPrincipal()).thenReturn(principal);
        Mockito.when(principal.getName()).thenReturn(name);
        return request;
    }
    
    @Test
    public void userPrincipalMatchesGroup() {
        GroupServletLimiter limiter = new GroupServletLimiter(
             VegasLimit.newDefault(), 
             GroupServletLimiter.fromUserPrincipal().andThen(principalToGroup::get),
             groupToPercent);

        HttpServletRequest request = createMockRequestWithPrincipal("bob");
        Optional<Listener> listener = limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(1)).get("bob");
    }

    @Test
    public void userPrincipalDoesNotMatchGroup() {
        GroupServletLimiter limiter = new GroupServletLimiter(
             VegasLimit.newDefault(), 
             GroupServletLimiter.fromUserPrincipal().andThen(principalToGroup::get),
             groupToPercent);

        HttpServletRequest request = createMockRequestWithPrincipal("doesntexist");
        Optional<Listener> listener = limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.times(1)).get("doesntexist");
    }

    @Test
    public void nullUserPrincipal() {
        GroupServletLimiter limiter = new GroupServletLimiter(
             VegasLimit.newDefault(), 
             GroupServletLimiter.fromUserPrincipal(),
             groupToPercent);

        HttpServletRequest request = createMockRequestWithPrincipal(null);
        Optional<Listener> listener =  limiter.acquire(request);
        
        Assert.assertTrue(listener.isPresent());
        Mockito.verify(principalToGroup, Mockito.never());
    }
}

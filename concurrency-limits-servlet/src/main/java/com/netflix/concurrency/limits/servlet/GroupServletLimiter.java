package com.netflix.concurrency.limits.servlet;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

/**
 * {@link ServletLimiter} that partitions the limit based on a single property of the request
 * such as a header, parameter, attribute or user principal. This property is then mapped to a group, 
 * where each group is allocated a certain percentage of the limit.
 * 
 * For example, the following setup guarantees 10% of the limit to batch and 90% to live based on the user
 * principal.  The user principal is mapped here to a group using a simple {@literal Map<String, String>} which
 * could have been loaded from a configuration file or database.
 * 
 * {@code
 * Map<String, String> principalToLiveOrBatch = ...;
 * Map<String, Double> groupToPercent = new HashMap<>();
 * groupToPercent.put("live", 0.9);
 * groupToPercent.put("batch", 0.1);
 *  
 * GroupServletServerLimiter limiter = new GroupServletServerLimiter(
 *     VegasLimit.newDefault(), 
 *     ServletServerLimiterImpl.fromUserPrincipal().andThen(principalToLiveOrBatch::get),
 *     groupToPercent);
 * }
 */
public final class GroupServletLimiter implements ServletLimiter {
    private final Function<HttpServletRequest, String> requestToGroup;
    private final Map<String, Integer> groupToIndex = new HashMap<>();
    private final Limiter<Integer> limiter;
    private final int defaultIndex;
    
    public static final Function<HttpServletRequest, String> fromUserPrincipal() {
        return request -> Optional.ofNullable(request.getUserPrincipal())
                .map(Principal::getName)
                .orElse(null);
    }
    
    public static final Function<HttpServletRequest, String> fromHeader(String name) {
        return request -> Optional.ofNullable(request.getHeader(name)).orElse(null);
    }
    
    public static final Function<HttpServletRequest, Object> fromAttribute(String name) {
        return request -> Optional.ofNullable(request.getAttribute(name)).orElse(null);
    }
    
    public static final Function<HttpServletRequest, String> fromParameter(String name) {
        return request -> Optional.ofNullable(request.getParameter(name)).orElse(null);
    }
    
    public static final Function<HttpServletRequest, String> fromPathInfo() {
        return request -> Optional.ofNullable(request.getPathInfo()).orElse(null);
    }
    
    public GroupServletLimiter(Limit limit, Function<HttpServletRequest, String> requestToGroup, Map<String, Double> groupPercentage) {
        Preconditions.checkArgument(limit != null, "limit cannot be null");
        Preconditions.checkArgument(requestToGroup != null, "requestToGroup cannot be null");
        Preconditions.checkArgument(groupPercentage != null, "groupPercentage cannot be null");
        
        groupPercentage.forEach((group, percent) -> groupToIndex.put(group, groupToIndex.size()));
        defaultIndex = groupToIndex.size();
        this.requestToGroup = requestToGroup;
        this.limiter = new DefaultLimiter<>(limit, new PercentageStrategy(
                Stream.concat(groupPercentage.values().stream(), Stream.of(0.0)).collect(Collectors.toList())));
    }
    
    @Override
    public Optional<Listener> acquire(HttpServletRequest request) {
        return limiter.acquire(Optional.ofNullable(requestToGroup.apply(request))
            .map(groupToIndex::get)
            .orElse(defaultIndex));
    }
}

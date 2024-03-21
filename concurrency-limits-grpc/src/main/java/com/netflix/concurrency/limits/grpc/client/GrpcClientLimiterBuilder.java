/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import io.grpc.CallOptions;

/**
 * Builder to simplify creating a {@link Limiter} specific to GRPC clients. 
 */
public final class GrpcClientLimiterBuilder extends AbstractPartitionedLimiter.Builder<GrpcClientLimiterBuilder, GrpcClientRequestContext> {
    private boolean blockOnLimit = false;

    public GrpcClientLimiterBuilder partitionByMethod() {
        return partitionResolver(context -> context.getMethod().getFullMethodName());
    }
    
    public GrpcClientLimiterBuilder partitionByCallOption(CallOptions.Key<String> option) {
        return partitionResolver(context -> context.getCallOptions().getOption(option));
    }

    /**
     * Bypass limit if the request's full method name matches the specified gRPC method's full name.
     * @param fullMethodName The full method name to check against the {@link GrpcClientRequestContext}'s method.
     *                       If the request's method matches this fullMethodName, the limit will be bypassed.
     * @return Chainable builder
     */
    public GrpcClientLimiterBuilder bypassLimitByMethod(String fullMethodName) {
        return bypassLimitResolver(context -> fullMethodName.equals(context.getMethod().getFullMethodName()));
    }

    /**
     * Bypass limit if the value of the specified call option matches the provided value.
     * @param option The call option key to check against the {@link GrpcClientRequestContext}'s call options.
     * @param value The value to compare against the value of the specified call option in the request.
     *              If they match, the limit will be bypassed.
     * @param <T> The type of the call option value.
     * @return Chainable builder
     */
    public <T> GrpcClientLimiterBuilder bypassLimitByCallOption(CallOptions.Key<String> option, T value) {
        return bypassLimitResolver(context -> value.equals(context.getCallOptions().getOption(option)));
    }
    
    /**
     * When set to true new calls to the channel will block when the limit has been reached instead
     * of failing fast with an UNAVAILABLE status. 
     * @param blockOnLimit
     * @return Chainable builder
     */
    public <T> GrpcClientLimiterBuilder blockOnLimit(boolean blockOnLimit) {
        this.blockOnLimit = blockOnLimit;
        return this;
    }
    
    @Override
    protected GrpcClientLimiterBuilder self() {
        return this;
    }
    
    public Limiter<GrpcClientRequestContext> build() {
        Limiter<GrpcClientRequestContext> limiter = super.build();

        if (blockOnLimit) {
            limiter = BlockingLimiter.wrap(limiter);
        }
        return limiter;
    }
}

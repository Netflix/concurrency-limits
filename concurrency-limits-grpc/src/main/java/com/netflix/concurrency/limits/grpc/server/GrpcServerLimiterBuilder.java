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
package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import io.grpc.Attributes;
import io.grpc.Metadata;
import java.util.function.Predicate;

public class GrpcServerLimiterBuilder extends AbstractPartitionedLimiter.Builder<GrpcServerLimiterBuilder, GrpcServerRequestContext> {
    /**
     * Partition the limit by method
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByMethod() {
        return partitionResolver((GrpcServerRequestContext context) -> context.getCall().getMethodDescriptor().getFullMethodName());
    }

    /**
     * Partition the limit by a request header.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByHeader(Metadata.Key<String> header) {
        return partitionResolver(context -> context.getHeaders().get(header));
    }

    /**
     * Partition the limit by a request attribute.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder partitionByAttribute(Attributes.Key<String> attribute) {
        return partitionResolver(context -> context.getCall().getAttributes().get(attribute));
    }


    /**
     * Add a chainable bypass resolver predicate from context. Multiple resolvers may be added and if any of the
     * predicate condition returns true the call is bypassed without increasing the limiter inflight count and
     * affecting the algorithm. Will not bypass any calls by default if no resolvers are added.
     *
     * @param shouldBypass Predicate condition to bypass limit
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder bypassLimitResolver(Predicate<GrpcServerRequestContext> shouldBypass) {
        return bypassLimitResolverInternal(shouldBypass);
    }

    /**
     * Bypass limit if the request's full method name matches the specified gRPC method's full name.
     * @param fullMethodName The full method name to check against the {@link GrpcServerRequestContext}'s method.
     *                       If the request's method matches this fullMethodName, the limit will be bypassed.
     * @return Chainable builder
     */
    public GrpcServerLimiterBuilder bypassLimitByMethod(String fullMethodName) {
        return bypassLimitResolver(context  -> fullMethodName.equals(context.getCall().getMethodDescriptor().getFullMethodName()));
    }

    /**
     * Bypass limit if the value of the specified header matches the provided value.
     * @param header The header key to check against the {@link GrpcServerRequestContext}'s headers.
     * @param value The value to compare against the value of the specified header in the request.
     *              If they match, the limit will be bypassed.
     * @param <T> The type of the header value.
     * @return Chainable builder
     */
    public <T> GrpcServerLimiterBuilder bypassLimitByHeader(Metadata.Key<String> header, T value) {
        return bypassLimitResolver(context -> value.equals(context.getHeaders().get(header)));
    }

    /**
     * Bypass limit if the value of the specified attribute matches the provided value.
     * @param attribute The attribute key to check against the {@link GrpcServerRequestContext}'s attributes.
     * @param value The value to compare against the value of the specified attribute in the request.
     *              If they match, the limit will be bypassed.
     * @param <T> The type of the attribute value.
     * @return Chainable builder
     */
    public <T> GrpcServerLimiterBuilder bypassLimitByAttribute(Attributes.Key<String> attribute, T value) {
        return bypassLimitResolver(context -> value.equals(context.getCall().getAttributes().get(attribute)));
    }

    @Override
    protected GrpcServerLimiterBuilder self() {
        return this;
    }
}

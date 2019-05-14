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

    @Override
    protected GrpcServerLimiterBuilder self() {
        return this;
    }
}

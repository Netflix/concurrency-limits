package com.netflix.concurrency.limits.grpc.server;

import io.grpc.Metadata;
import io.grpc.ServerCall;

public interface GrpcServerRequestContext {
    ServerCall<?, ?> getCall();
    Metadata getHeaders();
}
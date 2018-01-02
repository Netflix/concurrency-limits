package com.netflix.concurrency.limits.grpc.client;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

public interface GrpcClientRequestContext {
    MethodDescriptor<?, ?> getMethod();
    CallOptions getCallOptions();
}

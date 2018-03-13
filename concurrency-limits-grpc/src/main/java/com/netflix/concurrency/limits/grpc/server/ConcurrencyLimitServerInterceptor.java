package com.netflix.concurrency.limits.grpc.server;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.concurrency.limits.Limiter;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * {@link ServerInterceptor} that enforces per service and/or per method concurrent request limits and returns
 * a Status.UNAVAILABLE when that limit has been reached. 
 */
public class ConcurrencyLimitServerInterceptor implements ServerInterceptor {
    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Concurrency limit reached");

    private final Limiter<GrpcServerRequestContext> grpcLimiter;
    
    public ConcurrencyLimitServerInterceptor(Limiter<GrpcServerRequestContext> grpcLimiter) {
        this.grpcLimiter = grpcLimiter;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
                                                      final Metadata headers,
                                                      final ServerCallHandler<ReqT, RespT> next) {
        
        final Optional<Limiter.Listener> listener = grpcLimiter.acquire(new GrpcServerRequestContext() {
            @Override
            public ServerCall<?, ?> getCall() {
                return call;
            }

            @Override
            public Metadata getHeaders() {
                return headers;
            }
        });
        
        if (!listener.isPresent()) {
            call.close(LIMIT_EXCEEDED_STATUS, new Metadata());
            return new ServerCall.Listener<ReqT>() {};
        }

        final AtomicBoolean done = new AtomicBoolean(false);
        return next.startCall(
                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        try {
                            super.close(status, trailers);
                        } finally {
                            if (done.compareAndSet(false, true)) {
                                if (status.isOk()) {
                                    listener.get().onSuccess();
                                } else {
                                    listener.get().onIgnore();
                                }
                            }
                        }
                    }
                },
                headers);
    }

}

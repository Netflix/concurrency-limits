package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.LimiterRegistry;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ServerInterceptor} that enforces per service and/or per method concurrent request limits and returns
 * a Status.UNAVAILABLE when that limit has been reached. 
 */
public class ConcurrencyLimitServerInterceptor<ContextT> implements ServerInterceptor {
    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Concurrency limit reached");

    private final LimiterRegistry<ContextT> registry;
    private final ServerContextResolver<ContextT> contextResolver;

    public ConcurrencyLimitServerInterceptor(LimiterRegistry<ContextT> registry, ServerContextResolver<ContextT> contextResolver) {
        this.registry = registry;
        this.contextResolver = contextResolver;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
                                                      final Metadata headers,
                                                      final ServerCallHandler<ReqT, RespT> next) {
        
        final Limiter<ContextT> limiter = registry.get(call.getMethodDescriptor().getFullMethodName());
        final Optional<Limiter.Listener> listener = limiter.acquire(contextResolver.resolve(call.getMethodDescriptor(), headers));
        
        if (!listener.isPresent()) {
            call.close(LIMIT_EXCEEDED_STATUS, new Metadata());
            return new ServerCall.Listener<ReqT>() {};
        }

        final AtomicBoolean done = new AtomicBoolean(false);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                next.startCall(
                        new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                            public void close(Status status, Metadata trailers) {
                                try {
                                    super.close(status, trailers);
                                } finally {
                                    if (done.compareAndSet(false, true)) {
                                        if (status.isOk()) {
                                            listener.get().onSuccess();
                                        } else if (Code.UNAVAILABLE == status.getCode()) {
                                            listener.get().onDropped();
                                        } else {
                                            listener.get().onIgnore();
                                        }
                                    }
                                }
                            }
                        },
                        headers)) {
            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    if (done.compareAndSet(false, true)) {
                        listener.get().onIgnore();
                    }
                }
            }
        };
    }

}

package com.netflix.concurrency.limits.grpc.client;

import com.google.common.base.Preconditions;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.LimiterRegistry;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * ClientInterceptor that enforces per service and/or per method concurrent request limits and returns
 * a Status.UNAVAILABLE when that limit has been reached.  
 */
public class ConcurrencyLimitClientInterceptor<ContextT> implements ClientInterceptor {
    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Concurrency limit reached");
    
    private final LimiterRegistry<ContextT> registry;
    private final ClientContextResolver<ContextT> contextResolver;
    
    public ConcurrencyLimitClientInterceptor(LimiterRegistry<ContextT> registry, ClientContextResolver<ContextT> contextResolver) {
        Preconditions.checkArgument(registry != null, "ConcurrencyLimit cannot not be null");
        this.registry = registry;
        this.contextResolver = contextResolver;
    }
    
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        final Limiter<ContextT> limiter = registry.get(method.getFullMethodName());

        // Perform the operation and release the limiter once done.  
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            final AtomicBoolean done = new AtomicBoolean(false);
            volatile Optional<Limiter.Listener> listener = Optional.empty();
            
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                listener = limiter.acquire(contextResolver.resolve(callOptions, headers));
                if (!listener.isPresent()) {
                    responseListener.onClose(LIMIT_EXCEEDED_STATUS, new Metadata());
                    return;
                }
                
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        try {
                            super.onClose(status, trailers);
                        } finally {
                            listener.ifPresent(l -> {
                                if (done.compareAndSet(false, true)) {
                                    if (status.isOk()) {
                                        l.onSuccess();
                                    } else if (Code.UNAVAILABLE == status.getCode()) {
                                        l.onDropped();
                                    } else {
                                        l.onIgnore();
                                    }
                                }
                            });
                        }
                    }
                }, headers);
            }
            
            @Override
            public void cancel(@Nullable String message, @Nullable Throwable cause) {
                try {
                    super.cancel(message, cause);
                } finally {
                    if (done.compareAndSet(false, true)) {
                        listener.ifPresent(Limiter.Listener::onIgnore);
                    }
                }
            }
        };
    }
}

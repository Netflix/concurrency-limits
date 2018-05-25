package com.netflix.concurrency.limits.grpc.server;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.netflix.concurrency.limits.Limiter;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
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

    private final Supplier<Status> statusSupplier;

    private Supplier<Metadata> trailerSupplier;
    
    public static class Builder {
        private Supplier<Status> statusSupplier = () -> LIMIT_EXCEEDED_STATUS;
        private Supplier<Metadata> trailerSupplier = Metadata::new;
        private final Limiter<GrpcServerRequestContext> grpcLimiter;
        
        public Builder(Limiter<GrpcServerRequestContext> grpcLimiter) {
            this.grpcLimiter = grpcLimiter;
        }
        
        /**
         * Supplier for the Status code to return when the concurrency limit has been reached.
         * A custom supplier could augment the response to include additional information about
         * the server or limit. The supplier can also be used to trigger additional metrics.
         * By default will return an UNAVAILABLE.
         * 
         * @param supplier
         * @return Chainable builder
         */
        public Builder statusSupplier(Supplier<Status> supplier) {
            this.statusSupplier = supplier;
            return this;
        }
        
        /**
         * Supplier for the Metadata to return when the concurrency limit has been reached.
         * A custom supplier may include additional metadata about the server or limit
         * 
         * @param supplier
         * @return Chainable builder
         */
        public Builder trailerSupplier(Supplier<Metadata> supplier) {
            this.trailerSupplier = supplier;
            return this;
        }
        
        public ConcurrencyLimitServerInterceptor build() {
            return new ConcurrencyLimitServerInterceptor(this);
        }
    }
    
    public static Builder newBuidler(Limiter<GrpcServerRequestContext> grpcLimiter) {
        return new Builder(grpcLimiter);
    }
    
    /**
     * @deprecated Use {@link ConcurrencyLimitServerInterceptor#newBuidler(Limiter)}
     * @param grpcLimiter
     */
    @Deprecated
    public ConcurrencyLimitServerInterceptor(Limiter<GrpcServerRequestContext> grpcLimiter) {
        this.grpcLimiter = grpcLimiter;
        this.statusSupplier = () -> LIMIT_EXCEEDED_STATUS;
        this.trailerSupplier = Metadata::new;
    }
    
    private ConcurrencyLimitServerInterceptor(Builder builder) {
        this.grpcLimiter = builder.grpcLimiter;
        this.statusSupplier = builder.statusSupplier;
        this.trailerSupplier = builder.trailerSupplier;
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
            call.close(statusSupplier.get(), trailerSupplier.get());
            return new ServerCall.Listener<ReqT>() {};
        }

        final AtomicBoolean done = new AtomicBoolean(false);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                next.startCall(
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

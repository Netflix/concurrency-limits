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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.concurrency.limits.Limiter;
import io.grpc.Context;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link ServerInterceptor} that enforces per service and/or per method concurrent request limits and returns
 * a Status.UNAVAILABLE when that limit has been reached. 
 */
public class ConcurrencyLimitServerInterceptor implements ServerInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrencyLimitServerInterceptor.class);

    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Server concurrency limit reached");

    private final Limiter<GrpcServerRequestContext> grpcLimiter;

    private final Supplier<Status> statusSupplier;

    private Supplier<Metadata> trailerSupplier;

    private static final Executor executor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("concurrency-limit-cleanup-%d")
                    .build());

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
    
    public static Builder newBuilder(Limiter<GrpcServerRequestContext> grpcLimiter) {
        return new Builder(grpcLimiter);
    }
    
    /**
     * @deprecated Use {@link ConcurrencyLimitServerInterceptor#newBuilder(Limiter)}
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

        if (!call.getMethodDescriptor().getType().serverSendsOneMessage() || !call.getMethodDescriptor().getType().clientSendsOneMessage()) {
            return next.startCall(call, headers);
        }

        return grpcLimiter
            .acquire(new GrpcServerRequestContext() {
                @Override
                public ServerCall<?, ?> getCall() {
                    return call;
                }

                @Override
                public Metadata getHeaders() {
                    return headers;
                }
            })
            .map(new Function<Limiter.Listener, Listener<ReqT>>() {
                final AtomicBoolean done = new AtomicBoolean(false);

                void safeComplete(Runnable action) {
                    if (done.compareAndSet(false, true)) {
                        try {
                            action.run();
                        } catch (Throwable t) {
                            LOG.error("Critical error releasing limit", t);
                        }
                    }
                }

                @Override
                public Listener<ReqT> apply(Limiter.Listener listener) {
                    Context.current().addListener(context -> safeComplete(listener::onIgnore), executor);

                    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                            next.startCall(
                                    new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                                        @Override
                                        public void close(Status status, Metadata trailers) {
                                            try {
                                                super.close(status, trailers);
                                            } finally {
                                                safeComplete(() -> {
                                                    switch (status.getCode()) {
                                                        case CANCELLED:
                                                        case DEADLINE_EXCEEDED:
                                                            listener.onDropped();
                                                            break;
                                                        default:
                                                            listener.onSuccess();
                                                            break;
                                                    }
                                                });
                                            }
                                        }
                                    },
                                    headers)) {

                        @Override
                        public void onMessage(ReqT message) {
                            try {
                                super.onMessage(message);
                            } catch (Throwable t) {
                                LOG.error("Uncaught exception. Force releasing limit. ", t);
                                safeComplete(listener::onIgnore);
                                throw t;
                            }
                        }

                        @Override
                        public void onHalfClose() {
                            try {
                                super.onHalfClose();
                            } catch (Throwable t) {
                                LOG.error("Uncaught exception. Force releasing limit. ", t);
                                safeComplete(listener::onIgnore);
                                throw t;
                            }
                        }

                        @Override
                        public void onCancel() {
                            try {
                                super.onCancel();
                            } finally {
                                safeComplete(listener::onIgnore);
                            }
                        }
                    };
                }
            })
            .orElseGet(() -> {
                call.close(statusSupplier.get(), trailerSupplier.get());
                return new ServerCall.Listener<ReqT>() {};
            });
    }
}

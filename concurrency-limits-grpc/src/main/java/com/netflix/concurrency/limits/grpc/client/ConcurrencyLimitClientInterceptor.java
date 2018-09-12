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

import com.google.common.base.Preconditions;
import com.netflix.concurrency.limits.Limiter;

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
public class ConcurrencyLimitClientInterceptor implements ClientInterceptor {
    private static final Status LIMIT_EXCEEDED_STATUS = Status.UNAVAILABLE.withDescription("Concurrency limit reached");
    
    private final Limiter<GrpcClientRequestContext> grpcLimiter;
    
    public ConcurrencyLimitClientInterceptor(final Limiter<GrpcClientRequestContext> grpcLimiter) {
        Preconditions.checkArgument(grpcLimiter != null, "GrpcLimiter cannot not be null");
        this.grpcLimiter = grpcLimiter;
    }
    
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
        final Optional<Limiter.Listener> listener = grpcLimiter.acquire(new GrpcClientRequestContext() {
            @Override
            public MethodDescriptor<?, ?> getMethod() {
                return method;
            }

            @Override
            public CallOptions getCallOptions() {
                return callOptions;
            }
        });
        
        if (!listener.isPresent()) {
            return new ClientCall<ReqT, RespT>() {
                @Override
                public void start(io.grpc.ClientCall.Listener<RespT> responseListener, Metadata headers) {
                    responseListener.onClose(LIMIT_EXCEEDED_STATUS, new Metadata());
                }

                @Override
                public void request(int numMessages) {
                }

                @Override
                public void cancel(String message, Throwable cause) {
                }

                @Override
                public void halfClose() {
                }

                @Override
                public void sendMessage(ReqT message) {
                }
            };
        }
        
        // Perform the operation and release the limiter once done.  
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            final AtomicBoolean done = new AtomicBoolean(false);
            
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onClose(final Status status, final Metadata trailers) {
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
            public void cancel(final @Nullable String message, final @Nullable Throwable cause) {
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

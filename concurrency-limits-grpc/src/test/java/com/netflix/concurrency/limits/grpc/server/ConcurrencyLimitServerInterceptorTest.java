package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.grpc.client.GrpcClientLimiterBuilder;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.spectator.SpectatorMetricRegistry;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ConcurrencyLimitServerInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    final Limiter<GrpcServerRequestContext> limiter = Mockito.mock(Limiter.class);
    final Limiter.Listener listener = Mockito.mock(Limiter.Listener.class);

    @Before
    public void beforeEachTest() {
        Mockito.when(limiter.acquire(Mockito.isA(GrpcServerRequestContext.class))).thenReturn(Optional.of(listener));
    }

    private Server startServer(ServerCalls.UnaryMethod<String, String> method) {
        try {
            return NettyServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            ServerServiceDefinition.builder("service")
                                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
                                    .build(),
                            ConcurrencyLimitServerInterceptor.newBuilder(limiter)
                                    .build())
                    )
                    .build()
                    .start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void releaseOnSuccess() {
        // Setup server
        final Server server = startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        });

        // Make Client call
        final Channel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext(true)
                .build();

        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener, Mockito.times(1)).onSuccess();
    }

    @Test
    public void releaseOnError() {
        // Setup server
        final Server server = startServer((req, observer) -> {
            observer.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        });

        // Make Client call
        final Channel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext(true)
                .build();

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            // Verify
            Assert.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
            Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
            Mockito.verify(listener, Mockito.times(1)).onIgnore();
        }
    }

    @Test
    public void releaseOnUncaughtException() throws IOException {
        // Setup server
        final Server server = startServer((req, observer) -> {
            throw new RuntimeException("failure");
        });

        // Make Client call
        final Channel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext(true)
                .build();

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            // Verify
            Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
            Mockito.verify(listener, Mockito.times(1)).onIgnore();
        }
    }
}

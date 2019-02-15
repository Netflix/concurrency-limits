package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.mockito.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
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
import io.grpc.stub.ServerCalls;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class ConcurrencyLimitServerInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    final Limiter<GrpcServerRequestContext> limiter = Mockito.spy(SimpleLimiter.newBuilder().build());
    final OptionalResultCaptor<Limiter.Listener> listener = OptionalResultCaptor.forClass(Limiter.Listener.class);

    @Before
    public void beforeEachTest() {
        Mockito.doAnswer(listener).when(limiter).acquire(Mockito.any());
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
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onSuccess();
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
            Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
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
            Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
        }
    }
}

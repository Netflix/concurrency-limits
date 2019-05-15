package com.netflix.concurrency.limits.grpc.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.mockito.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ConcurrencyLimitServerInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    private Server server;
    private Channel channel;

    final Limiter<GrpcServerRequestContext> limiter = Mockito.spy(SimpleLimiter.newBuilder().build());
    final OptionalResultCaptor<Limiter.Listener> listener = OptionalResultCaptor.forClass(Limiter.Listener.class);

    @Before
    public void beforeEachTest() {
        Mockito.doAnswer(listener).when(limiter).acquire(Mockito.any());
    }

    @After
    public void afterEachTest() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void startServer(ServerCalls.UnaryMethod<String, String> method) {
        try {
            server = NettyServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            ServerServiceDefinition.builder("service")
                                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
                                    .build(),
                            ConcurrencyLimitServerInterceptor.newBuilder(limiter)
                                    .build())
                    )
                    .build()
                    .start();

            channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext(true)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void releaseOnSuccess() {
        // Setup server
        startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        });

        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onSuccess();
    }

    @Test
    public void releaseOnError() {
        // Setup server
        startServer((req, observer) -> {
            observer.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        });

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
    }

    @Test
    public void releaseOnUncaughtException() throws IOException {
        // Setup server
        startServer((req, observer) -> {
            throw new RuntimeException("failure");
        });

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
    }

    @Test
    public void releaseOnCancellation() {
        // Setup server
        startServer((req, observer) -> {});

        ListenableFuture<String> future = ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "foo");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        future.cancel(true);
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
    }

    @Test
    public void releaseOnDeadlineExceeded() {
        // Setup server
        startServer((req, observer) -> {});

        try {
            ClientCalls.blockingUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withDeadlineAfter(1, TimeUnit.SECONDS)), "foo");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
    }

    @Test
    public void releaseOnMissingHalfClose() {
        // Setup server
        startServer((req, observer) -> {});

        ClientCall<String, String> call = channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withDeadlineAfter(500, TimeUnit.MILLISECONDS));
        call.start(new ClientCall.Listener<String>() {}, new Metadata());
        call.request(2);
        call.sendMessage("foo");

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();
    }
}

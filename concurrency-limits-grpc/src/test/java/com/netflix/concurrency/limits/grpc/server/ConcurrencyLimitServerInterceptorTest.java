package com.netflix.concurrency.limits.grpc.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.mockito.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.spectator.SpectatorMetricRegistry;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Meter;
import com.netflix.spectator.api.Timer;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class ConcurrencyLimitServerInterceptorTest {
    @Rule
    public TestName testName = new TestName();

    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    private DefaultRegistry registry = new DefaultRegistry();

    private Server server;
    private Channel channel;

    Limiter<GrpcServerRequestContext> limiter;
    OptionalResultCaptor<Limiter.Listener> listener;

    @Before
    public void beforeEachTest() {
        limiter = Mockito.spy(SimpleLimiter.newBuilder()
                .named(testName.getMethodName())
                .metricRegistry(new SpectatorMetricRegistry(registry, registry.createId("unit.test.limiter")))
                .build());

        listener = OptionalResultCaptor.forClass(Limiter.Listener.class);

        Mockito.doAnswer(listener).when(limiter).acquire(Mockito.any());
    }

    @After
    public void afterEachTest() {
        if (server != null) {
            server.shutdown();
        }

        System.out.println("COUNTERS:");
        registry.counters().forEach(t -> System.out.println("  " + t.id() + " " + t.count()));
        System.out.println("DISTRIBUTIONS:");
        registry.distributionSummaries().forEach(t -> System.out.println("  " + t.id() + " " + t.count() + " " + t.totalAmount()));
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

        verifyCounts(0, 0, 1, 0);
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

        verifyCounts(0, 0, 1, 0);
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

        verifyCounts(0, 1, 0, 0);
    }

    @Test
    public void releaseOnCancellation() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        });

        ListenableFuture<String> future = ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "foo");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        future.cancel(true);

        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0);
    }

    @Test
    public void releaseOnDeadlineExceeded() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        });

        try {
            ClientCalls.blockingUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withDeadlineAfter(1, TimeUnit.SECONDS)), "foo");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0);
    }

    public void verifyCounts(int dropped, int ignored, int success, int rejected) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(dropped, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "dropped").count());
        Assert.assertEquals(ignored, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "ignored").count());
        Assert.assertEquals(success, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "success").count());
        Assert.assertEquals(rejected, registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "rejected").count());
    }
}

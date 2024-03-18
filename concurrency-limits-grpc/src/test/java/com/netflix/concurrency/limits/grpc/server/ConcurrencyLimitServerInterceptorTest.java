package com.netflix.concurrency.limits.grpc.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.util.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.spectator.SpectatorMetricRegistry;
import com.netflix.spectator.api.DefaultRegistry;
import io.grpc.CallOptions;
import io.grpc.Channel;
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
import java.util.concurrent.TimeUnit;

import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.BYPASS_METHOD_DESCRIPTOR;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.METHOD_DESCRIPTOR;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.TEST_METRIC_NAME;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.verifyCounts;

public class ConcurrencyLimitServerInterceptorTest {
    @Rule
    public TestName testName = new TestName();

    Limiter<GrpcServerRequestContext> limiter;
    Limiter<GrpcServerRequestContext> bypassEnabledLimiter;
    OptionalResultCaptor<Limiter.Listener> listener;
    DefaultRegistry registry = new DefaultRegistry();
    SpectatorMetricRegistry spectatorMetricRegistry = new SpectatorMetricRegistry(registry, registry.createId(TEST_METRIC_NAME));
    private Server server;
    private Channel channel;

    @Before
    public void beforeEachTest() {
        limiter = Mockito.spy(SimpleLimiter.newBuilder()
                .named(testName.getMethodName())
                .metricRegistry(spectatorMetricRegistry)
                .build());

        bypassEnabledLimiter = Mockito.spy(new GrpcServerLimiterBuilder()
                .bypassLimitByMethod("service/bypass")
                .named(testName.getMethodName())
                .metricRegistry(spectatorMetricRegistry)
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

    private void startServer(ServerCalls.UnaryMethod<String, String> method, Limiter<GrpcServerRequestContext> limiter) {
        try {
            server = NettyServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            ServerServiceDefinition.builder("service")
                                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
                                    .addMethod(BYPASS_METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
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
        }, limiter);

        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void releaseOnError() {
        // Setup server
        startServer((req, observer) -> {
            observer.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        }, limiter);

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void releaseOnUncaughtException() throws IOException {
        // Setup server
        startServer((req, observer) -> {
            throw new RuntimeException("failure");
        }, limiter);

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            Assert.fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();

        verifyCounts(0, 1, 0, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void releaseOnCancellation() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        }, limiter);

        ListenableFuture<String> future = ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "foo");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        future.cancel(true);

        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void releaseOnDeadlineExceeded() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        }, limiter);

        try {
            ClientCalls.blockingUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withDeadlineAfter(1, TimeUnit.SECONDS)), "foo");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenBypassConditionAdded() {
        // Setup server with a bypass condition enabled limiter
        startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        }, bypassEnabledLimiter);

        // Calling a method for which the predicate condition passes
        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(bypassEnabledLimiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));

        verifyCounts(0, 0, 0, 0, 1, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenBypassConditionFailed() {
        // Setup server with a bypass condition enabled limiter
        startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        }, bypassEnabledLimiter);

        // Calling a method for which the predicate condition fails
        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(bypassEnabledLimiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenNoBypassConditionAdded() {
        // Setup server with no bypass condition enabled
        startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        }, limiter);

        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

}

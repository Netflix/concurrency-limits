package com.netflix.concurrency.limits.grpc.client;

import com.netflix.concurrency.limits.Limiter;

import com.netflix.concurrency.limits.grpc.util.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import com.netflix.concurrency.limits.spectator.SpectatorMetricRegistry;
import com.netflix.spectator.api.DefaultRegistry;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.BYPASS_METHOD_DESCRIPTOR;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.METHOD_DESCRIPTOR;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.TEST_METRIC_NAME;
import static com.netflix.concurrency.limits.grpc.util.InterceptorTestUtil.verifyCounts;

public class ConcurrencyLimitClientInterceptorTest {

    @Rule
    public TestName testName = new TestName();

    Limiter<GrpcClientRequestContext> limiter;
    SimpleLimiter<GrpcClientRequestContext> bypassEnabledLimiter;
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

        bypassEnabledLimiter = Mockito.spy(SimpleLimiter.<GrpcClientRequestContext>newBypassLimiterBuilder()
                .shouldBypass(new ClientBypassMethodPredicate())
                .named(testName.getMethodName())
                .metricRegistry(spectatorMetricRegistry)
                .build());

        listener = OptionalResultCaptor.forClass(Limiter.Listener.class);
        Mockito.doAnswer(listener).when(limiter).acquire(Mockito.any());
    }

    private void startServer(Limiter<GrpcClientRequestContext> limiter) {

        ServerCalls.UnaryMethod<String, String> method = (request, responseObserver) -> {
            responseObserver.onNext("response");
            responseObserver.onCompleted();
        };

        try {
            server = NettyServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            ServerServiceDefinition.builder("service")
                                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
                                    .addMethod(BYPASS_METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall(method))
                                    .build())
                    )
                    .build()
                    .start();

            channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext(true)
                    .intercept(new ConcurrencyLimitClientInterceptor(limiter))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void verifySuccessCountOnRelease() {
        // Setup server
        startServer(limiter);

        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");

        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcClientRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenBypassConditionAdded() {
        // Setup server with a bypass condition enabled limiter
        startServer(bypassEnabledLimiter);

        // Calling a method for which the predicate condition passes
        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(bypassEnabledLimiter, Mockito.times(1)).acquire(Mockito.isA(GrpcClientRequestContext.class));

        verifyCounts(0, 0, 0, 0, 1, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenBypassConditionFailed() {
        // Setup server with a bypass condition enabled limiter
        startServer(bypassEnabledLimiter);

        // Calling a method for which the predicate condition fails
        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(bypassEnabledLimiter, Mockito.times(1)).acquire(Mockito.isA(GrpcClientRequestContext.class));

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void verifyBypassCountWhenNoBypassConditionAdded() {
        // Setup server with no bypass condition enabled
        startServer(limiter);

        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcClientRequestContext.class));

        verifyCounts(0, 0, 1, 0, 0, registry, testName.getMethodName());
    }

    @Test
    public void testMultipleCalls() {
        // Setup server with a bypass condition enabled limiter
        startServer(bypassEnabledLimiter);

        // Calling both method types
        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        ClientCalls.blockingUnaryCall(channel, BYPASS_METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");

        Mockito.verify(bypassEnabledLimiter, Mockito.times(4)).acquire(Mockito.isA(GrpcClientRequestContext.class));

        verifyCounts(0, 0, 2, 0, 2, registry, testName.getMethodName());
    }
    
    @Test
    @Ignore
    public void simulation() throws IOException {
        Semaphore sem = new Semaphore(20, true);
        Server server = NettyServerBuilder.forPort(0)
            .addService(ServerServiceDefinition.builder("service")
                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall((req, observer) -> {
                        try {
                            sem.acquire();
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                        } finally {
                            sem.release();
                        }
                        
                        observer.onNext("response");
                        observer.onCompleted();
                    }))
                    .build())
            .build()
            .start();
        
        Limiter<GrpcClientRequestContext> limiter = new GrpcClientLimiterBuilder()
                .blockOnLimit(true)
                .build();
        
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .usePlaintext(true)
                .intercept(new ConcurrencyLimitClientInterceptor(limiter))
                .build();
        
        AtomicLong counter = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(" " + counter.getAndSet(0) + " : " + limiter.toString());
        }, 1, 1, TimeUnit.SECONDS);
        
        for (int i = 0 ; i < 10000000; i++) {
            counter.incrementAndGet();
            ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "request");
        }
    }

    public static class ClientBypassMethodPredicate implements Predicate<GrpcClientRequestContext> {
        @Override
        public boolean test(GrpcClientRequestContext grpcClientRequestContext) {
            return grpcClientRequestContext.getMethod().getFullMethodName().contains("bypass");
        }
    }
}

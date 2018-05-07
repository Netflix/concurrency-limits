package com.netflix.concurrency.limits.grpc.server.example;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ExponentialDistribution;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.netflix.concurrency.limits.limit.GradientLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.ServerCalls.UnaryMethod;
import io.grpc.stub.StreamObserver;

public class Example {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    
    public static ServerCallHandler<String, String> createServerHandler(int concurrency) {
        final ExponentialDistribution distribution = new ExponentialDistribution(10.0);
        final Supplier<Integer> latency = () -> 100 + (int)distribution.sample();
        
        List<Semaphore> semaphores = Arrays.asList(
                new Semaphore(concurrency, true),
                new Semaphore(concurrency*2, true),
                new Semaphore(concurrency*4, true),
                new Semaphore(concurrency*8, true));
        
        return ServerCalls.asyncUnaryCall(new UnaryMethod<String, String>() {
            AtomicInteger counter = new AtomicInteger();
            @Override
            public void invoke(String req, StreamObserver<String> observer) {
                Semaphore sem = semaphores.get((counter.incrementAndGet() / 2000) % semaphores.size());
                try {
                    sem.acquire();
                    Uninterruptibles.sleepUninterruptibly(latency.get(), TimeUnit.MILLISECONDS);
                    observer.onNext("response");
                    observer.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    observer.onError(Status.UNKNOWN.asRuntimeException());
                } finally {
                    sem.release();
                }
            }
        });
    }
    
    public static void main(String[] args) throws IOException {
        Limit limit = VegasLimit.newBuilder()
                .build();
        
        // Create a server
        Server server = NettyServerBuilder.forPort(0)
            .addService(ServerInterceptors.intercept(ServerServiceDefinition.builder("service")
                    .addMethod(METHOD_DESCRIPTOR, createServerHandler(10))
                    .build(), new ConcurrencyLimitServerInterceptor(new GrpcServerLimiterBuilder()
                            .limiter(builder -> builder
                                    .limit(limit)
                                    .minWindowTime(200, TimeUnit.MILLISECONDS)
                                    )
                            .build())
                ))
            .build()
            .start();
        
        // Report progress
        AtomicInteger dropCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(MessageFormat.format("{0}, {1}, {2}", limit.getLimit(), successCount.getAndSet(0), dropCount.getAndSet(0)));
        }, 1, 1, TimeUnit.SECONDS);
        
        // Create a client
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .usePlaintext(true)
                .build();

        DriverBuilder.newBuilder()
            .exponential(3, 2, TimeUnit.SECONDS)
            .run(1, TimeUnit.HOURS, () -> {
                ClientCalls.asyncUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withWaitForReady()), "request",
                        new StreamObserver<String>() {
                            @Override
                            public void onNext(String value) {
                            }

                            @Override
                            public void onError(Throwable t) {
                                dropCount.incrementAndGet();
                            }

                            @Override
                            public void onCompleted() {
                                successCount.incrementAndGet();
                            }
                        
                    });
            });
    }
}

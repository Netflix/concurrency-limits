package com.netflix.concurrency.limits.grpc.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.LimiterRegistry;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;
import org.junit.Test;

public class ConcurrencyLimitClientInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.create(
            MethodType.UNARY, "service/method", StringMarshaller.INSTANCE, StringMarshaller.INSTANCE);
    
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
        
        Limiter<Void> limiter = BlockingLimiter.wrap(new DefaultLimiter<Void>(VegasLimit.newDefault(), new SimpleStrategy()));
        
        Channel channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .usePlaintext(true)
                .intercept(new ConcurrencyLimitClientInterceptor<>(LimiterRegistry.single(limiter), ClientContextResolver.none()))
                .build();
        
        AtomicLong counter = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(" " + counter.getAndSet(0) + " : " + limiter.toString());
        }, 1, 1, TimeUnit.SECONDS);
        
        for (int i = 0 ; i < 10000000; i++) {
            counter.incrementAndGet();
            ListenableFuture<String> future = ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "request");
        }
    }
}

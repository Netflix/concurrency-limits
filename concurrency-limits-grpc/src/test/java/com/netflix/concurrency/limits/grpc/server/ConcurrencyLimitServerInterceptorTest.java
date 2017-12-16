package com.netflix.concurrency.limits.grpc.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.LimiterRegistry;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.client.ClientContextResolver;
import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.PercentageStrategy;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.junit.Ignore;
import org.junit.Test;

public class ConcurrencyLimitServerInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.create(MethodType.UNARY,
            "service/method", StringMarshaller.INSTANCE, StringMarshaller.INSTANCE);

    private static final Key<String> ID_HEADER = Metadata.Key.of("id", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    @Ignore
    public void simulation() throws IOException, InterruptedException {
        Limiter<Integer> limiter = new DefaultLimiter<>(FixedLimit.of(50), new PercentageStrategy(Arrays.asList(0.1, 0.9)));
        
        Server server = NettyServerBuilder.forPort(0)
            .addService(ServerInterceptors.intercept(ServerServiceDefinition.builder("service")
                    .addMethod(METHOD_DESCRIPTOR, ServerCalls.asyncUnaryCall((req, observer) -> {
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                        }
                        
                        observer.onNext("response");
                        observer.onCompleted();
                    }))
                    .build(), new ConcurrencyLimitServerInterceptor<Integer>(LimiterRegistry.single(limiter), (method, header) -> {
                        return Integer.parseInt(header.get(ID_HEADER));
                    })
                ))
            .build()
            .start();
        
        Limiter<Void> clientLimiter0 = BlockingLimiter.wrap(new DefaultLimiter<Void>(new AIMDLimit(10), new SimpleStrategy()));
        Limiter<Void> clientLimiter1 = BlockingLimiter.wrap(new DefaultLimiter<Void>(new AIMDLimit(10), new SimpleStrategy()));
        
        AtomicLongArray counters = new AtomicLongArray(2);
        AtomicLong drops = new AtomicLong(0);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println("" + drops.getAndSet(0) + " : " + limiter.toString());
            System.out.println("  0: " + counters.getAndSet(0, 0) + " " + clientLimiter0);
            System.out.println("  1: " + counters.getAndSet(1, 0) + " " + clientLimiter1);
        }, 1, 1, TimeUnit.SECONDS);
        
        Executor executor = Executors.newCachedThreadPool();

        executor.execute(() -> simulateClient(0, counters, drops, server.getPort(), clientLimiter0));
        executor.execute(() -> simulateClient(1, counters, drops, server.getPort(), clientLimiter1));
        
        TimeUnit.SECONDS.sleep(100);
    }

    private void simulateClient(int id, AtomicLongArray counters, AtomicLong drops, int port, Limiter limiter) {
        Metadata headers = new Metadata();
        headers.put(ID_HEADER, "" + id);

        Channel channel = NettyChannelBuilder.forTarget("localhost:" + port).usePlaintext(true)
                .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
                .intercept(new ConcurrencyLimitClientInterceptor<>(LimiterRegistry.single(limiter),
                        ClientContextResolver.none()))
                .build();

        try {
            while (true) {
                TimeUnit.MICROSECONDS.sleep(100);
                ClientCalls.asyncUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT.withWaitForReady()), "request",
                    new StreamObserver<String>() {
                        @Override
                        public void onNext(String value) {
                        }

                        @Override
                        public void onError(Throwable t) {
                            drops.incrementAndGet();
                        }

                        @Override
                        public void onCompleted() {
                            counters.incrementAndGet(id);
                        }
                    
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

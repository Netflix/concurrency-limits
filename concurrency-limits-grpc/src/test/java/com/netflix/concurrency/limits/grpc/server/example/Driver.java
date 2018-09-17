package com.netflix.concurrency.limits.grpc.server.example;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Driver {
    public static final Metadata.Key<String> ID_HEADER = Metadata.Key.of("id", Metadata.ASCII_STRING_MARSHALLER);

    private interface Segment {
        long duration();
        long nextDelay();
        String name();
    }
    
    static public Builder newBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private List<Driver.Segment> segments = new ArrayList<>();
        private int port;
        private long runtimeSeconds;
        private Consumer<Long> latencyAccumulator;
        private String id = "";

        public Builder normal(double mean, double sd, long duration, TimeUnit units) {
            final NormalDistribution distribution = new NormalDistribution(mean, sd);
            return add("normal(" + mean + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder uniform(double lower, double upper, long duration, TimeUnit units) {
            final UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
            return add("uniform(" + lower + "," + upper + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder exponential(double mean, long duration, TimeUnit units) {
            final ExponentialDistribution distribution = new ExponentialDistribution(mean);
            return add("exponential(" + mean + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder exponentialRps(double rps, long duration, TimeUnit units) {
            return exponential(1000.0 / rps, duration, units);
        }
        
        public Builder slience(long duration, TimeUnit units) {
            return add("slience()", () -> units.toMillis(duration), duration, units);
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder latencyAccumulator(Consumer<Long> consumer) {
            this.latencyAccumulator = consumer;
            return this;
        }
        
        public Builder runtime(long duration, TimeUnit units) {
            this.runtimeSeconds = units.toNanos(duration);
            return this;
        }
        
        public Builder add(String name, Supplier<Long> delaySupplier, long duration, TimeUnit units) {
            segments.add(new Segment() {
                @Override
                public long duration() {
                    return units.toNanos(duration);
                }
    
                @Override
                public long nextDelay() {
                    return delaySupplier.get();
                }
    
                @Override
                public String name() {
                    return name;
                }
            });
            return this;
        }
        
        public Driver build() {
            return new Driver(this);
        }
    }

    private final List<Segment> segments;
    private final Channel channel;
    private final long runtime;
    private final Consumer<Long> latencyAccumulator;
    private final AtomicInteger successCounter = new AtomicInteger(0);
    private final AtomicInteger dropCounter = new AtomicInteger(0);

    public Driver(Builder builder) {
        this.segments = builder.segments;
        this.runtime = builder.runtimeSeconds;
        this.latencyAccumulator = builder.latencyAccumulator;

        Metadata metadata = new Metadata();
        metadata.put(ID_HEADER, builder.id);

        this.channel = ClientInterceptors.intercept(NettyChannelBuilder.forTarget("localhost:" + builder.port)
                .usePlaintext(true)
                .build(),
                    MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    public int getAndResetSuccessCount() { return successCounter.getAndSet(0); }
    public int getAndResetDropCount() { return dropCounter.getAndSet(0); }

    public CompletableFuture<Void> runAsync() {
        return CompletableFuture.runAsync(this::run, Executors.newSingleThreadExecutor());
    }

    public void run() {
        long endTime = System.nanoTime() + this.runtime;
        while (true) {
            for (Driver.Segment segment : segments) {
                long segmentEndTime = System.nanoTime() + segment.duration();
                while (true) {
                    long currentTime = System.nanoTime();
                    if (currentTime > endTime) {
                        return;
                    }
                    
                    if (currentTime > segmentEndTime) {
                        break;
                    }
                    
                    long startTime = System.nanoTime();
                    Uninterruptibles.sleepUninterruptibly(Math.max(0, segment.nextDelay()), TimeUnit.MILLISECONDS);
                    ClientCalls.asyncUnaryCall(channel.newCall(TestServer.METHOD_DESCRIPTOR, CallOptions.DEFAULT.withWaitForReady()), "request",
                            new StreamObserver<String>() {
                                @Override
                                public void onNext(String value) {
                                }

                                @Override
                                public void onError(Throwable t) {
                                    dropCounter.incrementAndGet();
                                }

                                @Override
                                public void onCompleted() {
                                    latencyAccumulator.accept(System.nanoTime() - startTime);
                                    successCounter.incrementAndGet();
                                }
                        });
                }
            }
        }
    }
}
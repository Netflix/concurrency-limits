package com.netflix.concurrency.limits.grpc.server.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

import com.google.common.util.concurrent.Uninterruptibles;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

public class Driver {
    private static interface Segment {
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
        private Runnable dropAction;
        private Runnable successAction;
        private Consumer<Long> latencyAccumulator;
        
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
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder dropAction(Runnable action) {
            this.dropAction = action;
            return this;
        }
        
        public Builder successAction(Runnable action) {
            this.successAction = action;
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
    private final ManagedChannel channel;
    private final long runtime;
    private final Runnable successAction;
    private final Runnable dropAction;
    private final Consumer<Long> latencyAccumulator;
    
    public Driver(Builder builder) {
        this.segments = builder.segments;
        this.runtime = builder.runtimeSeconds;
        this.successAction = builder.successAction;
        this.dropAction = builder.dropAction;
        this.latencyAccumulator = builder.latencyAccumulator;
        this.channel = NettyChannelBuilder.forTarget("localhost:" + builder.port)
                .usePlaintext(true)
                .build();
        
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
                                    dropAction.run();
                                }

                                @Override
                                public void onCompleted() {
                                    latencyAccumulator.accept(System.nanoTime() - startTime);
                                    successAction.run();
                                }
                        });
                }
            }
        }
    }
}
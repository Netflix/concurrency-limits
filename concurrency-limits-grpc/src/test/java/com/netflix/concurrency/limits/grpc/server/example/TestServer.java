package com.netflix.concurrency.limits.grpc.server.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerRequestContext;

import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.ServerCalls.UnaryMethod;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServer {
    private static final Logger LOG = LoggerFactory.getLogger(TestServer.class);

    public static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    private interface Segment {
        long duration();
        long latency();
        String name();
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private List<TestServer.Segment> segments = new ArrayList<>();
        private int concurrency = 2;
        private Limiter<GrpcServerRequestContext> limiter;
        
        public Builder limiter(Limiter<GrpcServerRequestContext> limiter) {
            this.limiter = limiter;
            return this;
        }
        
        public Builder concurrency(int concurrency) {
            this.concurrency  = concurrency;
            return this;
        }
        
        public Builder exponential(double mean, long duration, TimeUnit units) {
            final ExponentialDistribution distribution = new ExponentialDistribution(mean);
            return add("exponential(" + mean + ")", () -> (long)distribution.sample(), duration, units);
        }
        
        public Builder lognormal(long mean, long duration, TimeUnit units) {
            final LogNormalDistribution distribution = new LogNormalDistribution(3.0,1.0);
            final double distmean = distribution.getNumericalMean();
            return add("lognormal(" + mean + ")", () -> (long)(distribution.sample() * mean / distmean), duration, units);
        }
        
        public Builder slience(long duration, TimeUnit units) {
            return add("slience()", () -> units.toMillis(duration), duration, units);
        }
    
        public Builder add(String name, Supplier<Long> latencySupplier, long duration, TimeUnit units) {
            segments.add(new Segment() {
                @Override
                public long duration() {
                    return units.toNanos(duration);
                }
    
                @Override
                public long latency() {
                    return latencySupplier.get();
                }
    
                @Override
                public String name() {
                    return name;
                }
            });
            return this;
        }
    
        public TestServer build() throws IOException {
            return new TestServer(this);
        }
        
    }
    
    private final Semaphore semaphore;
    private final Server server;
    
    private TestServer(final Builder builder) throws IOException {
        this.semaphore = new Semaphore(builder.concurrency, true);
        
        ServerCallHandler<String, String> handler = ServerCalls.asyncUnaryCall(new UnaryMethod<String, String>() {
            volatile int segment = 0;
            
            {
                Executors.newSingleThreadExecutor().execute(() -> {
                    while (true) {
                        Segment s = builder.segments.get(0);
                        Uninterruptibles.sleepUninterruptibly(s.duration(), TimeUnit.NANOSECONDS);
                        segment = segment++ % builder.segments.size();
                    }
                });
            }
            
            @Override
            public void invoke(String req, StreamObserver<String> observer) {
                try {
                    long delay = builder.segments.get(0).latency();
                    semaphore.acquire();
                    TimeUnit.MILLISECONDS.sleep(delay);
                    observer.onNext("response");
                    observer.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    observer.onError(Status.UNKNOWN.asRuntimeException());
                } finally {
                    semaphore.release();
                }
            }
        });
        
        this.server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(ServerServiceDefinition.builder("service")
                        .addMethod(METHOD_DESCRIPTOR, handler)  // Rate = Limit / Latency = 2 / 0.02 = 100
                        .build(), ConcurrencyLimitServerInterceptor.newBuilder(builder.limiter)
                        .build()
                    ))
                .build()
                .start();
    }

    public int getPort() {
        return server.getPort();
    }
}

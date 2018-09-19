package com.netflix.concurrency.limits.grpc.server.example;

import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.WindowedLimit;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionedExample {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        final Gradient2Limit limit = Gradient2Limit.newBuilder()
                .build();
        
        // Create a server
        final TestServer server = TestServer.newBuilder()
            .concurrency(2)
            .lognormal(20, 1, TimeUnit.MINUTES)
            .limiter(
                new GrpcServerLimiterBuilder()
                        .partitionByHeader(Driver.ID_HEADER)
                        .partition("1", 1.0)
                        .partition("2", 0.0)
//                        .partition("3", 0.0)
//                        .partitionRejectDelay("2", 1000, TimeUnit.MILLISECONDS)
//                        .partitionRejectDelay("3", 1000, TimeUnit.MILLISECONDS)
                        .limit(WindowedLimit.newBuilder()
                                .minWindowTime(1, TimeUnit.SECONDS)
                                .windowSize(10)
                                .build(limit))
                .build()
                )
            .build();

        final LatencyCollector latency = new LatencyCollector();

        final Driver driver1 = Driver.newBuilder()
                .id("1")
                .exponentialRps(50, 60, TimeUnit.SECONDS)
                .latencyAccumulator(latency)
                .runtime(1, TimeUnit.HOURS)
                .port(server.getPort())
                .build();

        final Driver driver2 = Driver.newBuilder()
                .id("2")
                .exponentialRps(50,  60, TimeUnit.SECONDS)
                .exponentialRps(100, 60, TimeUnit.SECONDS)
                .latencyAccumulator(latency)
                .runtime(1, TimeUnit.HOURS)
                .port(server.getPort())
                .build();

        final Driver driver3 = Driver.newBuilder()
                .id("3")
                .exponentialRps(50, 60, TimeUnit.SECONDS)
                .latencyAccumulator(latency)
                .runtime(1, TimeUnit.HOURS)
                .port(server.getPort())
                .build();

        // Report progress
        final AtomicInteger counter = new AtomicInteger(0);
        System.out.println("iteration, limit, live, batch, live, batch, latency, shortRtt, longRtt");
//        System.out.println("iteration, limit, 70%, 20%, 10%, 70%, 20%, 10%, latency, shortRtt, longRtt");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(MessageFormat.format(
                    "{0,number,#}, {1,number,#}, {2,number,#}, {3,number,#}, {4,number,#}, {5,number,#}, {6,number,#}, {7,number,#}, {8,number,#}",
                    counter.incrementAndGet(), 
                    limit.getLimit(),
                    driver1.getAndResetSuccessCount(),
                    driver2.getAndResetSuccessCount(),
//                    driver3.getAndResetSuccessCount(),
                    driver1.getAndResetDropCount(),
                    driver2.getAndResetDropCount(),
//                    driver3.getAndResetDropCount(),
                    TimeUnit.NANOSECONDS.toMillis(latency.getAndReset()),
                    limit.getShortRtt(TimeUnit.MILLISECONDS),
                    limit.getLongRtt(TimeUnit.MILLISECONDS)
                    ))  ;
        }, 1, 1, TimeUnit.SECONDS);

        CompletableFuture.allOf(
                  driver1.runAsync()
                , driver2.runAsync()
//                , driver3.runAsync()
        ).get();
    }
}

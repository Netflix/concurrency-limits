package com.netflix.concurrency.limits.grpc.server.example;

import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.WindowedLimit;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Example {
    public static void main(String[] args) throws IOException {
        final Gradient2Limit limit = Gradient2Limit.newBuilder().build();

        // Create a server
        final TestServer server = TestServer.newBuilder()
            .concurrency(2)
            .lognormal(20, 1, TimeUnit.MINUTES)
            .limiter(
                new GrpcServerLimiterBuilder()
                        .limit(WindowedLimit.newBuilder()
                                .minWindowTime(1, TimeUnit.SECONDS)
                                .windowSize(10)
                                .build(limit))
                .build()
                )
            .build();

        final LatencyCollector latency = new LatencyCollector();

        final Driver driver = Driver.newBuilder()
            .exponentialRps(50,  100, TimeUnit.SECONDS)
            .exponentialRps(90,  100, TimeUnit.SECONDS)
            .exponentialRps(200, 100, TimeUnit.SECONDS)
            .exponentialRps(100, 100, TimeUnit.SECONDS)
            .latencyAccumulator(latency)
            .runtime(1, TimeUnit.HOURS)
            .port(server.getPort())
            .build();

        // Report progress
        final AtomicInteger counter = new AtomicInteger(0);
        System.out.println("iteration, limit, success, drop, latency, shortRtt, longRtt");
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                System.out.println(MessageFormat.format("{0,number,#}, {1,number,#}, {2,number,#}, {3,number,#}, {4,number,#}, {5,number,#}, {6,number,#}",
                        counter.incrementAndGet(),
                        limit.getLimit(),
                        driver.getAndResetSuccessCount(),
                        driver.getAndResetDropCount(),
                        TimeUnit.NANOSECONDS.toMillis(latency.getAndReset()),
                        limit.getLastRtt(TimeUnit.MILLISECONDS),
                        limit.getRttNoLoad(TimeUnit.MILLISECONDS)
                ));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);

        // Create a client
        driver.run();
    }
}

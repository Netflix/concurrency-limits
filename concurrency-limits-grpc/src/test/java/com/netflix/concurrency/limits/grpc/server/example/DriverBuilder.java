package com.netflix.concurrency.limits.grpc.server.example;

import com.google.common.util.concurrent.Uninterruptibles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class DriverBuilder {
    private static interface Segment {
        long duration();
        long nextDelay();
        String name();
    }
    
    static public DriverBuilder newBuilder() {
        return new DriverBuilder();
    }
    
    private List<DriverBuilder.Segment> segments = new ArrayList<>();
    
    public DriverBuilder normal(double mean, double sd, long duration, TimeUnit units) {
        final NormalDistribution distribution = new NormalDistribution(mean, sd);
        return add("normal(" + mean + ")", () -> (long)distribution.sample(), duration, units);
    }
    
    public DriverBuilder uniform(double lower, double upper, long duration, TimeUnit units) {
        final UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
        return add("uniform(" + lower + "," + upper + ")", () -> (long)distribution.sample(), duration, units);
    }
    
    public DriverBuilder exponential(double mean, long duration, TimeUnit units) {
        final ExponentialDistribution distribution = new ExponentialDistribution(mean);
        return add("exponential(" + mean + ")", () -> (long)distribution.sample(), duration, units);
    }
    
    public DriverBuilder slience(long duration, TimeUnit units) {
        return add("slience()", () -> units.toMillis(duration), duration, units);
    }
    
    public DriverBuilder add(String name, Supplier<Long> delaySupplier, long duration, TimeUnit units) {
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

    public void run(long runtime, TimeUnit units, Runnable action) {
        long endTime = System.nanoTime() + units.toNanos(runtime);
        while (true) {
            for (DriverBuilder.Segment segment : segments) {
                System.out.println(segment.name());
                long segmentEndTime = System.nanoTime() + segment.duration();
                while (true) {
                    long currentTime = System.nanoTime();
                    if (currentTime > endTime) {
                        return;
                    }
                    
                    if (currentTime > segmentEndTime) {
                        break;
                    }
                    
                    Uninterruptibles.sleepUninterruptibly(Math.max(0, segment.nextDelay()), TimeUnit.MILLISECONDS);
                    action.run();
                }
            }
        }
    }
}
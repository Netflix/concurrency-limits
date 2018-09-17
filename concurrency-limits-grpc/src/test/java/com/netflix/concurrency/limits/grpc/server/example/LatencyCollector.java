package com.netflix.concurrency.limits.grpc.server.example;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class LatencyCollector implements Consumer<Long> {
    private static class Metrics {
        long count;
        long total;

        public Metrics() {
            this(0, 0);
        }

        public Metrics(long count, long total) {
            this.count = count;
            this.total = total;
        }

        public long average() {
            if (this.count == 0)
                return 0;
            return this.total / this.count;
        }
    }

    AtomicReference<Metrics> foo = new AtomicReference<Metrics>(new Metrics());

    @Override
    public void accept(Long sample) {
        foo.getAndUpdate(current -> new Metrics(current.count + 1, current.total + sample));
    }

    public long getAndReset() {
        return foo.getAndSet(new Metrics()).average();
    }
}

package com.netflix.concurrency.limits.limit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class Gradient2LimitTest {

    @Test
    public void testDefault() {
        Gradient2Limit limit = Gradient2Limit.newBuilder().initialLimit(20).build();
        assertEquals(20, limit.getLimit());
    }

    @Test
    public void recoversTowardInitialLimit_afterTransientLatencySpike_evenAtLowInflight() {
        Gradient2Limit limit = Gradient2Limit.newBuilder()
                .initialLimit(20)
                .minLimit(5)
                .maxConcurrency(100)
                .queueSize(4)
                .rttTolerance(1.5)
                .build();

        // Establish a healthy RTT baseline, kept short so the limit doesn't drift far from 20.
        for (int i = 0; i < 5; i++) {
            limit.onSample(0, 1_000_000L, 18, false);
        }

        // Latency spike at high inflight: shortRtt jumps far above the slow-moving longRtt
        // baseline, forcing repeated shrink. inflight stays above limit / 2 so the gate stays
        // open throughout the spike.
        for (int i = 0; i < 20; i++) {
            limit.onSample(0, 50_000_000L, 50, false);
        }
        int shrunkLimit = limit.getLimit();
        assertTrue("limit should have shrunk below initialLimit during the spike", shrunkLimit < 20);

        // Latency recovers, but inflight stays low (below the shrunk limit / 2). Before this
        // fix the "app limited" gate would stay closed forever here, freezing the limit at
        // shrunkLimit. Below initialLimit the gate must stay open so the limit can climb back.
        for (int i = 0; i < 100; i++) {
            limit.onSample(0, 1_000_000L, 2, false);
        }

        assertTrue("limit should recover above its post-spike value", limit.getLimit() > shrunkLimit);
    }
}

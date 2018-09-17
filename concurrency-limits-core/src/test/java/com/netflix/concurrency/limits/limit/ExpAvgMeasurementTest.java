package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;
import org.junit.Assert;
import org.junit.Test;

public class ExpAvgMeasurementTest {
    @Test
    public void testWarmup() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(10, 10, Math::min);

        for (int i = 0; i < 10; i++) {
            avg.add(i + 10);
            Assert.assertEquals(10.0, avg.get().doubleValue(), 0.01);
        }

        avg.add(100);
        Assert.assertEquals(26.36, avg.get().doubleValue(), 0.01);
    }
}

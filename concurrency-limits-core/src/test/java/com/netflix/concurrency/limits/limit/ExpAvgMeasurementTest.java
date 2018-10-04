package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ExpAvgMeasurementTest {
    @Test
    public void testWarmup() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);

        double expected[] = new double[]{10.0, 10, 10, 10, 10, 10, 10, 10, 10, 10};
        for (int i = 0; i < 10; i++) {
            double value = avg.add(i + 10).doubleValue();
            Assert.assertEquals(expected[i], avg.get().doubleValue(), 0.01);
        }

        avg.add(100);
        Assert.assertEquals(11.7, avg.get().doubleValue(), 0.1);
    }
}

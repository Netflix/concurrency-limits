package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;

import org.junit.Assert;
import org.junit.Test;

public class ExpAvgMeasurementTest {
    @Test
    public void testWarmup() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);

        double expected[] = new double[]{10.0, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 14.5};
        for (int i = 0; i < 10; i++) {
            double value = avg.add(i + 10).doubleValue();
            Assert.assertEquals(expected[i], avg.get().doubleValue(), 0.01);
        }

        avg.add(100);
        Assert.assertEquals(16.2, avg.get().doubleValue(), 0.1);
    }

    @Test
    public void testUpdateBeforeFirstSample() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);

        avg.update(value -> 50);

        Assert.assertEquals(50.0, avg.get().doubleValue(), 0.0001);
        Assert.assertEquals(50.990099, avg.add(100).doubleValue(), 0.0001);
    }

    @Test
    public void testUpdateUsesExponentialAverageAcrossWarmupBoundary() {
        for (int samples : new int[]{1, 9, 10, 11}) {
            ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);
            for (int i = 0; i < samples; i++) {
                avg.add(100);
            }

            avg.update(value -> value.doubleValue() / 2);

            Assert.assertEquals("samples=" + samples,
                    50.990099, avg.add(100).doubleValue(), 0.0001);
            Assert.assertEquals("samples=" + samples,
                    51.960592, avg.add(100).doubleValue(), 0.0001);
        }
    }

    @Test
    public void testRepeatedUpdates() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(3, 10);
        avg.add(100);

        avg.update(value -> value.doubleValue() / 2);
        avg.update(value -> value.doubleValue() + 10);

        Assert.assertEquals(80.0, avg.add(100).doubleValue(), 0.0001);
    }

    @Test
    public void testResetRestoresWarmupAfterUpdate() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);
        avg.add(10);
        avg.update(value -> 50);

        avg.reset();

        Assert.assertEquals(0.0, avg.get().doubleValue(), 0.0001);
        Assert.assertEquals(100.0, avg.add(100).doubleValue(), 0.0001);
        Assert.assertEquals(150.0, avg.add(200).doubleValue(), 0.0001);
    }

    @Test
    public void testFailedUpdatePreservesWarmup() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(100, 10);
        avg.add(100);
        RuntimeException failure = new RuntimeException("update failed");

        try {
            avg.update(value -> { throw failure; });
            Assert.fail("Expected update to fail");
        } catch (RuntimeException exception) {
            Assert.assertSame(failure, exception);
        }

        Assert.assertEquals(100.0, avg.get().doubleValue(), 0.0001);
        Assert.assertEquals(150.0, avg.add(200).doubleValue(), 0.0001);
    }
}

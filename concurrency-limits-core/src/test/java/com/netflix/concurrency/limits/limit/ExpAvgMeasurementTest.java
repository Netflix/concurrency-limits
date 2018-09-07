package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;
import org.junit.Test;

public class ExpAvgMeasurementTest {
    @Test
    public void test() {
        ExpAvgMeasurement avg = new ExpAvgMeasurement(10);

        for (int i = 0; i < 10; i++) {
            avg.add(1);
        }

        avg.add(10);
    }
}

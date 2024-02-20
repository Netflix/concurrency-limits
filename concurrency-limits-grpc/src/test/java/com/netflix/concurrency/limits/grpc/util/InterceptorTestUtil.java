package com.netflix.concurrency.limits.grpc.util;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.grpc.MethodDescriptor;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class InterceptorTestUtil {

    public static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("service/method")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    public static final MethodDescriptor<String, String> BYPASS_METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("service/bypass")
            .setRequestMarshaller(StringMarshaller.INSTANCE)
            .setResponseMarshaller(StringMarshaller.INSTANCE)
            .build();

    public static final String TEST_METRIC_NAME = "unit.test.limiter";

    public static void verifyCounts(int dropped, int ignored, int success, int rejected, int bypassed, Registry registry, String idTagValue) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(dropped, registry.counter(TEST_METRIC_NAME + ".call", "id", idTagValue, "status", "dropped").count());
        Assert.assertEquals(ignored, registry.counter(TEST_METRIC_NAME + ".call", "id", idTagValue, "status", "ignored").count());
        Assert.assertEquals(success, registry.counter(TEST_METRIC_NAME + ".call", "id", idTagValue, "status", "success").count());
        Assert.assertEquals(rejected, registry.counter(TEST_METRIC_NAME + ".call", "id", idTagValue, "status", "rejected").count());
        Assert.assertEquals(bypassed, registry.counter(TEST_METRIC_NAME + ".call", "id", idTagValue, "status", "bypassed").count());
    }


}

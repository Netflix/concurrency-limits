package com.netflix.concurrency.limits.executors;

import static org.mockito.Mockito.when;

import com.netflix.concurrency.limits.Limiter;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BlockingAdaptiveExecutorTest {

    @Mock
    private Limiter<Void> limiter;

    @Test
    public void shouldBlock() {
        when(limiter.acquire(null)).thenReturn(Optional.empty());

        BlockingAdaptiveExecutor executor = BlockingAdaptiveExecutor.newBuilder().limiter(limiter).executor(Runnable::run).build();

        AtomicBoolean executed = new AtomicBoolean();
        executor.execute(() -> executed.set(true));

        Assert.assertTrue(executed.get());
    }
}

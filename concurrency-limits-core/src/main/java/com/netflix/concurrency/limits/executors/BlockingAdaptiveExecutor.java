package com.netflix.concurrency.limits.executors;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@link Executor} which uses a {@link Limiter} to determine the size of the thread pool.
 * Any {@link Runnable} executed once the limit has been reached will block the calling
 * thread until the limit is released.
 * 
 * Operations submitted to this executor should be homogeneous and have similar
 * long term latency characteristics.  RTT samples will only be taken from successful 
 * operations.  The {@link Runnable} should throw a {@link RejectedExecutionException} if 
 * a request timed out or some external limit was reached.  All other exceptions will be
 * ignored.
 */
public final class BlockingAdaptiveExecutor implements Executor {
    private final Limiter<Void> limiter;
    private final Executor executor;

    public BlockingAdaptiveExecutor(Limiter<Void> limiter) {
        this(limiter, Executors.newCachedThreadPool());
    }

    public BlockingAdaptiveExecutor(Limiter<Void> limiter, Executor executor) {
        this.limiter = BlockingLimiter.wrap(limiter);
        this.executor = executor;
    }

    @Override
    public void execute(Runnable command) {
        Listener token = limiter.acquire(null).orElseThrow(() -> new RejectedExecutionException());
        executor.execute(() -> {
            try {
                command.run();
                token.onSuccess();
            } catch (RejectedExecutionException e) {
                token.onDropped();
            } catch (Exception e) {
                token.onIgnore();
            }
        });
    }
}

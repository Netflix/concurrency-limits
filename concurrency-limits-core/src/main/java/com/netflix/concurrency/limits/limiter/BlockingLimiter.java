/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * {@link Limiter} that blocks the caller when the limit has been reached.  The caller is
 * blocked until the limiter has been released, or a timeout is reached.  This limiter is
 * commonly used in batch clients that use the limiter as a back-pressure mechanism.
 * 
 * @param <ContextT>
 */
public final class BlockingLimiter<ContextT> implements Limiter<ContextT> {
    public static final Duration MAX_TIMEOUT = Duration.ofHours(1);

    /**
     * Wrap a limiter such that acquire will block up to {@link BlockingLimiter#MAX_TIMEOUT} if the limit was reached
     * instead of return an empty listener immediately
     * @param delegate Non-blocking limiter to wrap
     * @return Wrapped limiter
     */
    public static <ContextT> BlockingLimiter<ContextT> wrap(Limiter<ContextT> delegate) {
        return new BlockingLimiter<>(delegate, MAX_TIMEOUT);
    }

    /**
     * Wrap a limiter such that acquire will block up to a provided timeout if the limit was reached
     * instead of return an empty listener immediately
     *
     * @param delegate Non-blocking limiter to wrap
     * @param timeout Max amount of time to wait for the wait for the limit to be released.  Cannot exceed {@link BlockingLimiter#MAX_TIMEOUT}
     * @return Wrapped limiter
     */
    public static <ContextT> BlockingLimiter<ContextT> wrap(Limiter<ContextT> delegate, Duration timeout) {
        Preconditions.checkArgument(timeout.compareTo(MAX_TIMEOUT) < 0, "Timeout cannot be greater than " + MAX_TIMEOUT);
        return new BlockingLimiter<>(delegate, timeout);
    }

    private final Limiter<ContextT> delegate;
    private final Duration timeout;
    
    /**
     * Lock used to block and unblock callers as the limit is reached
     */
    private final Object lock = new Object();

    private BlockingLimiter(Limiter<ContextT> limiter, Duration timeout) {
        this.delegate = limiter;
        this.timeout = timeout;
    }
    
    private Optional<Listener> tryAcquire(ContextT context) {
        final Instant deadline = Instant.now().plus(timeout);
        synchronized (lock) {
            while (true) {
                long timeout = Duration.between(Instant.now(), deadline).toMillis();
                if (timeout <= 0) {
                    return Optional.empty();
                }
                // Try to acquire a token and return immediately if successful
                final Optional<Listener> listener = delegate.acquire(context);
                if (listener.isPresent()) {
                    return listener;
                }
                
                // We have reached the limit so block until a token is released
                try {
                    lock.wait(timeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }
    }
    
    private void unblock() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }
    
    @Override
    public Optional<Listener> acquire(ContextT context) {
        return tryAcquire(context).map(delegate -> new Listener() {
            @Override
            public void onSuccess() {
                delegate.onSuccess();
                unblock();
            }

            @Override
            public void onIgnore() {
                delegate.onIgnore();
                unblock();
            }

            @Override
            public void onDropped() {
                delegate.onDropped();
                unblock();
            }
        });
    }
    
    @Override
    public String toString() {
        return "BlockingLimiter [" + delegate + "]";
    }
}

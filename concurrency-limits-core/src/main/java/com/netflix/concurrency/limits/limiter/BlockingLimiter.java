package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Limiter that blocks the caller when the limit has been reached.  The caller is
 * blocked until the limiter has been released. 
 * 
 * @param <ContextT>
 */
public final class BlockingLimiter<ContextT> implements Limiter<ContextT> {
    public static <ContextT> BlockingLimiter<ContextT> wrap(Limiter<ContextT> delegate) {
        return new BlockingLimiter<ContextT>(delegate);
    }
    /**
     * Lock used to block and unblock callers as the limit is reached
     */
    private final Lock lock = new ReentrantLock(true);
    
    /**
     * Lock used to block and unblock callers as the limit is reached
     */
    private final Condition notBlocked  = lock.newCondition();

    private final Limiter<ContextT> delegate;

    private boolean blocked = false;

    private BlockingLimiter(Limiter<ContextT> limiter) {
        this.delegate = limiter;
    }
    
    @Override
    public Optional<Listener> acquire(ContextT context) {
        return callUnderLock(() -> {
            while (true) {
                Optional<Listener> listener = delegate.acquire(context);
                if (listener.isPresent()) {
                    return listener.map(original -> {
                        return new Listener() {
                            @Override
                            public void onSuccess() {
                                original.onSuccess();
                                unblock();
                            }

                            @Override
                            public void onIgnore() {
                                original.onIgnore();
                                unblock();
                            }

                            @Override
                            public void onDropped() {
                                original.onDropped();
                                unblock();
                            }
                            
                            private void unblock() {
                                callUnderLock(() -> {
                                    if (blocked) {
                                        blocked = false;
                                        notBlocked.signal();
                                    }
                                });
                            }
                        };
                    });
                } else {
                    blocked = true;
                    
                    try {
                        notBlocked.await();
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        return Optional.empty();
                    }
                }
            }
        });
    }
    
    private <S> S callUnderLock(Callable<S> callable) {
        lock.lock();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void callUnderLock(Runnable action) {
        callUnderLock(() -> { action.run(); return null; });
    }
}

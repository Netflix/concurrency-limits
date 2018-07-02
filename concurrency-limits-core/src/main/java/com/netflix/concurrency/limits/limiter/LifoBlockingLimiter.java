package com.netflix.concurrency.limits.limiter;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.netflix.concurrency.limits.Limiter;

/**
 * {@link Limiter} decorator that blocks the caller when the limit has been reached.  This 
 * strategy ensures the resource is properly protected but favors availability over latency
 * by not fast failing requests when the limit has been reached.  To help keep success latencies
 * low and minimize timeouts any blocked requests are processed in last in/first out order.  
 * 
 * Use this limiter only when the threading model allows the limiter to be blocked. 
 * 
 * @param <ContextT>
 */
public final class LifoBlockingLimiter<ContextT> implements Limiter<ContextT> {
    public static class Builder<ContextT> {
        
        private final Limiter<ContextT> delegate;
        private int maxBacklogSize = 100;
        private Function<ContextT, Long> maxBacklogTimeoutMillis = context -> 1_000L;
        
        private Builder(Limiter<ContextT> delegate) {
            this.delegate = delegate;
        }

        /**
         * Set maximum number of blocked threads
         * 
         * @param size New max size.  Default is 100.
         * @return Chainable builder
         */
        public Builder<ContextT> maxBacklogSize(int size) {
            this.maxBacklogSize = size;
            return this;
        }
        
        /**
         * Set maximum timeout for threads blocked on the limiter.
         * Default is 1 second.
         * 
         * @param timeout
         * @param units
         * @return Chainable builder
         */
        public Builder<ContextT> backlogTimeout(long timeout, TimeUnit units) {
            return backlogTimeoutMillis(units.toMillis(timeout));
        }
        
        /**
         * Set maximum timeout for threads blocked on the limiter.
         * Default is 1 second.
         * 
         * @param timeout
         * @return Chainable builder
         */
        public Builder<ContextT> backlogTimeoutMillis(long timeout) {
            this.maxBacklogTimeoutMillis = context -> timeout;
            return this;
        }

        /**
         * Function to derive the backlog timeout from the request context.  This allows timeouts
         * to be set dynamically based on things like request deadlines. 
         * @param supplier
         * @param units
         * @return
         */
        public Builder<ContextT> backlogTimeout(Function<ContextT, Long> mapper, TimeUnit units) {
            this.maxBacklogTimeoutMillis = context -> units.toMillis(mapper.apply(context));
            return this;
        }

        public LifoBlockingLimiter<ContextT> build() {
            return new LifoBlockingLimiter<ContextT>(this);
        }
    }
    
    public static <ContextT> Builder<ContextT> newBuilder(Limiter<ContextT> delegate) {
        return new Builder<ContextT>(delegate);
    }
    
    private final Limiter<ContextT> delegate;
    
    private static class Event<ContextT> {
        private volatile Optional<Listener> listener;
        private final CountDownLatch latch = new CountDownLatch(1);
        private ContextT context;
        
        public Event(ContextT context) {
            this.context = context;
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
        
        public void set(Optional<Listener> listener) {
            this.listener = listener;
            latch.countDown();
        }
        
    }
    
    /**
     * Lock used to block and unblock callers as the limit is reached
     */
    private final Deque<Event<ContextT>> backlog = new LinkedList<>();
    
    private final AtomicInteger backlogCounter = new AtomicInteger();
    
    private final int backlogSize;
    
    private final Function<ContextT, Long> backlogTimeoutMillis;
    
    private final Object lock = new Object();
    
    private LifoBlockingLimiter(Builder<ContextT> builder) {
        this.delegate = builder.delegate;
        this.backlogSize = builder.maxBacklogSize;
        backlogTimeoutMillis = builder.maxBacklogTimeoutMillis;
    }
    
    private Optional<Listener> tryAcquire(ContextT context) {
        // Try to acquire a token and return immediately if successful
        final Optional<Listener> listener = delegate.acquire(context);
        if (listener.isPresent()) {
            return listener;
        }

        // Otherwise block until the limit has been released or we timeout 
        if (backlogCounter.get() > this.backlogSize) {
            return Optional.empty();
        }
        backlogCounter.incrementAndGet();
        final Event<ContextT> event = new Event<>(context);
        
        try {
            synchronized (lock) {
                backlog.addFirst(event);
            }
            
            if (!event.await(backlogTimeoutMillis.apply(context), TimeUnit.MILLISECONDS)) {
                synchronized (lock) {
                    backlog.removeFirstOccurrence(event);
                }
                return Optional.empty();
            }
            return event.listener;
        } catch (InterruptedException e) {
            synchronized (lock) {
                backlog.removeFirstOccurrence(event);
            }
            Thread.currentThread().interrupt();
            return Optional.empty();
        } finally {
            backlogCounter.decrementAndGet();
        }
    }
    
    private void unblock() {
        synchronized (lock) {
            if (!backlog.isEmpty()) {
                final Event<ContextT> event = backlog.peekFirst();
                final Optional<Listener> listener = delegate.acquire(event.context);
                if (listener.isPresent()) {
                    backlog.removeFirst();
                    event.set(listener);
                } else {
                    // Still can't acquire the limit.  unblock will be called again next time
                    // the limit is released.
                    return;
                }
            }
        }
    }
    
    @Override
    public Optional<Listener> acquire(ContextT context) {
        return tryAcquire(context).map(delegate -> {
            return new Listener() {
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
            };
        });
    }
    
    @Override
    public String toString() {
        return "BlockingLimiter [" + delegate + "]";
    }
}

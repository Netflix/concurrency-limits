package com.netflix.concurrency.limits.limiter;

import java.util.Optional;

public class SimpleLimiter<ContextT> extends AbstractLimiter<ContextT> {
    public static class Builder<ContextT> extends AbstractLimiter.Builder<Builder<ContextT>, ContextT> {
        public SimpleLimiter build() {
            return new SimpleLimiter(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

    public static <ContextT> Builder<ContextT> newBuilder() {
        return new Builder<ContextT>();
    }

    public SimpleLimiter(AbstractLimiter.Builder<?, ContextT> builder) {
        super(builder);
    }

    @Override
    public Optional<Listener> acquire(ContextT context) {
        if (getInflight() > getLimit()) {
            return Optional.empty();
        }
        return Optional.of(createListener());
    }
}

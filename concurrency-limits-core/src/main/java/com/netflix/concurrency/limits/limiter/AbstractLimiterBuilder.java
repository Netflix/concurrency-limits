package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.strategy.LookupPartitionStrategy;
import com.netflix.concurrency.limits.strategy.LookupPartitionStrategy.Builder;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class through which an RPC specific limiter may be constructed
 *
 * @param <BuilderT> Concrete builder type specific to an RPC mechanism
 * @param <ContextT> Type capturing the context of a request for the concrete RPC mechanism
 */
public abstract class AbstractLimiterBuilder<BuilderT extends AbstractLimiterBuilder<BuilderT, ContextT>, ContextT> {
    protected Strategy<ContextT> strategy;
    protected Builder<ContextT> builder;
    protected Limit limit = VegasLimit.newDefault();
    
    /**
     * Configure the strategy for partitioning the limit.  
     * @param contextToGroup Mapper from the context to a name group associated with a percentage of the limit
     * @param configurer Function through which the value set support by the func may be assigned
     *                   a percentage of the limit
     * @return Chainable builder
     */
    public BuilderT partitionByLookup(
            Function<ContextT, String> contextToGroup, 
            Consumer<LookupPartitionStrategy.Builder<ContextT>> configurer) {
        Preconditions.checkState(this.strategy == null && this.builder == null, "strategy already set");
        
        builder = LookupPartitionStrategy.<ContextT>newBuilder(contextToGroup);
        configurer.accept(builder);
        return self();
    }
    
    /**
     * Strategy for acquiring a token from the limiter based on the context.
     * @param strategy
     * @return Chainable builder
     */
    public BuilderT strategy(Strategy<ContextT> strategy) {
        Preconditions.checkState(this.strategy == null && this.builder == null, "strategy already set");
        
        this.strategy = strategy;
        return self();
    }
    
    /**
     * Set the limit algorithm to use.  Default is {@link VegasLimit}
     * @param limit Limit algorithm to use
     * @return Chainable builder
     */
    public BuilderT limit(Limit limit) {
        this.limit = limit;
        return self();
    }
    
    protected Strategy<ContextT> getFinalStrategy() {
        if (builder != null) {
            return builder.build();
        } else if (strategy != null) {
            return strategy;
        } else {
            return new SimpleStrategy<>();
        }
    }

    protected abstract BuilderT self();
}

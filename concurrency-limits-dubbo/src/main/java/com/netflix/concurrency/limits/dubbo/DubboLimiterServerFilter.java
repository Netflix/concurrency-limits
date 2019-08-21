/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.dubbo;

import com.netflix.concurrency.limits.Limiter;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;

import java.util.Optional;

import static com.netflix.concurrency.limits.dubbo.DubboLimiterServerFilter.CONCURRENCY_LIMITER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;

/**
 * Limiter filter used for provider side
 */
@Activate(group = {PROVIDER}, value = CONCURRENCY_LIMITER_KEY)
public class DubboLimiterServerFilter extends ListenableFilter {
    public static final String CONCURRENCY_LIMITER_KEY = "concurrency.limit.server";

    private DubboLimiterFactory limiterFactory;

    public DubboLimiterServerFilter() {
        super.listener = new ConcurrencyLimitListener();
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if (limiterFactory != null) {
            limiterFactory.getLimiter(invoker.getUrl())
                    .acquire(invocation)
                    .ifPresent(l -> RpcContext.getServerContext().set(CONCURRENCY_LIMITER_KEY, l));
        }
        return invoker.invoke(invocation);
    }

    public void setLimiterFactory(DubboLimiterFactory limiterFactory) {
        this.limiterFactory = limiterFactory;
    }

    static class ConcurrencyLimitListener implements Listener {
        @Override
        public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
            getListener().ifPresent(Limiter.Listener::onSuccess);
        }

        @Override
        public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
            getListener().ifPresent(l -> {
                if (t instanceof RpcException) {
                    RpcException rpcException = (RpcException) t;
                    if (!rpcException.isBiz()) {
                        l.onDropped();
                    }
                }
            });
        }

        private Optional<Limiter.Listener> getListener() {
            Limiter.Listener limit = (Limiter.Listener) RpcContext.getServerContext().get(CONCURRENCY_LIMITER_KEY);
            return Optional.ofNullable(limit);
        }
    }
}

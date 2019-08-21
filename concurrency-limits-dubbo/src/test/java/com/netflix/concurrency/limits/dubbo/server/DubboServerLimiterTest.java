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
package com.netflix.concurrency.limits.dubbo.server;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.dubbo.DubboLimiterFactory;
import com.netflix.concurrency.limits.dubbo.DubboLimiterServerFilter;
import org.apache.curator.test.TestingServer;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.rpc.Invocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author guohaoice@gmail.com
 */
public class DubboServerLimiterTest {
    private TestingServer zkServer;

    @Test
    @Disabled
    public void simulation() {
        // framework
        ApplicationConfig app = new ApplicationConfig("dubbo-demo-api-provider");
        // registry
        RegistryConfig registryConfig = new RegistryConfig("zookeeper://127.0.0.1:" + zkServer.getPort());

        // provider
        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
        service.setApplication(app);
        service.setRegistry(registryConfig);
        service.setInterface(DemoService.class);
        service.setParameters(new HashMap<>());
        service.getParameters().put(DubboLimiterServerFilter.CONCURRENCY_LIMITER_KEY, "true");
        service.setRef(new DemoServiceImpl());
        service.export();

        // consumer
        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(app);
        reference.setRegistry(registryConfig);
        reference.setInterface(DemoService.class);
        reference.setAsync(true);
        DemoService ref = reference.get();
        DubboLimiterFactory factory = ExtensionLoader.getExtensionLoader(DubboLimiterFactory.class)
                .getAdaptiveExtension();

        URL providerURL = new URLBuilder().setPath("com.netflix.concurrency.limits.dubbo.DubboLimiterTest$DemoService")
                .addParameter(CommonConstants.SIDE_KEY, CommonConstants.PROVIDER_SIDE)
                .build();

        Limiter<Invocation> limiter = factory.getLimiter(providerURL);

        AtomicLong counter = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(" " + counter.getAndSet(0) + " : " + limiter.toString());
        }, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < 1000000; i++) {
            counter.incrementAndGet();
            ref.echo("dubbo");
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        this.zkServer = new TestingServer(true);
        this.zkServer.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    interface DemoService {
        CompletableFuture<String> echo(String input);
    }

    static class DemoServiceImpl implements DemoService {
        Semaphore sem = new Semaphore(20, true);

        ExecutorService executorService = Executors.newFixedThreadPool(50);

        @Override
        public CompletableFuture<String> echo(String input) {

            return CompletableFuture.supplyAsync(() -> {
                try {
                    sem.acquire();
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    // ignored
                } finally {
                    sem.release();
                }
                return input;
            }, executorService);
        }
    }
}

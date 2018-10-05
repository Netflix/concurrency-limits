# Overview

Java Library that implements and integrates concepts from TCP congestion control to auto-detect concurrency limits for services in order to achieve optimal throughput with optimal latency.

# Background

When thinking of service availability operators traditionally think in terms of RPS (requests per second). Stress tests are normally performed to determine the RPS at which point the service tips over. RPS limits are then set somewhere below this tipping point (say 75% of this value) and enforced via a token bucket. However, in large distributed systems that auto-scale this value quickly goes out of date and the service falls over by becoming non-responsive as it is unable to gracefully shed excess load. Instead of thinking in terms of RPS, we should be thinking in terms of concurrent request where we apply queuing theory to determine the number of concurrent requests a service can handle before a queue starts to build up, latencies increase and the service eventually exhausts a hard limit such as CPU, memory, disk or network. This relationship is covered very nicely with Little's Law where `Limit = Average RPS * Average Latency`.

Concurrency limits are very easy to enforce but difficult to determine as they would require operators to fully understand the hardware services run on and coordinate how they scale. Instead we'd prefer to measure or estimate the concurrency limits at each point in the network.  As systems scale and hit limits each node will adjust and enforce its local view of the limit. To estimate the limit we borrow from common TCP congestion control algorithms by equating a system's concurrency limit to a TCP congestion window. 

Before applying the algorithm we need to set some ground rules. 
* We accept that every system has an inherent concurrency limit that is determined by a hard resources, such as number of CPU cores. 
* We accept that this limit can change as a system auto-scales.  
* For large and complex distributed systems it's impossible to know all the hard resources.
* We can use latency measurements to determine when queuing happens.
* We can use timeouts and rejected requests to aggressively back off.

# Limit algorithms

## Vegas

Delay based algorithm where the bottleneck queue is estimated as 

    L * (1 - minRTT/sampleRtt)
    
At the end of each sampling window the limit is increased by 1 if the queue is less than alpha (typically a value between 2-3) or decreased by 1 if the queue is greater than beta (typically a value between 4-6 requests)

## Gradient2

This algorithm attempts to address bias and drift when using minimum latency measurements. To do this the algorithm tracks uses the measure of divergence between two exponential averages over a long and short time time window. Using averages the algorithm can smooth out the impact of outliers for bursty traffic. Divergence duration is used as a proxy to identify a queueing trend at which point the algorithm aggresively reduces the limit.

# Enforcement Strategies

## Simple

In the simplest use case we don't want to differentiate between requests and so enforce a single gauge of the number of inflight requests.  Requests are rejected immediately once the gauge value equals the limit.

## Percentage

For more complex systems it's desirable to provide certain quality of service guarantees while still making efficient use of resources.  Here we guarantee specific types of requests get a certain percentage of the concurrency limit.  For example, a system that takes both live and batch traffic may want to give live traffic 100% of the limit during heavy load and is OK with starving batch traffic. Or, a system may want to guarantee that 50% of the limit is given to write traffic so writes are never starved.

# Integrations

## GRPC

A concurrency limiter may be installed either on the server or client. The choice of limiter depends on your use case. For the most part it is recommended to use a dynamic delay based limiter such as the VegasLimit on the server and either a pure loss based (AIMDLimit) or combined loss and delay based limiter on the client.

### Server limiter

The purpose of the server limiter is to protect the server from either increased client traffic (batch apps or retry storms) or latency spikes from a dependent service.  With the limiter installed the server can ensure that latencies remain low by rejecting excess traffic with `Status.UNAVAILABLE` errors.

In this example a GRPC server is configured with a single adaptive limiter that is shared among batch and live traffic with live traffic guaranteed 90% of throughput and 10% guaranteed to batch.  For simplicity we just expect the client to send a "group" header identifying it as 'live' or 'batch'.  Ideally this should be done using TLS certificates and a server side lookup of identity to grouping.  Any requests not identified as either live or batch may only use excess capacity. 

```java
// Create and configure a server builder
ServerBuilder builder = ...;

builder.addService(ServerInterceptor.intercept(service,
    ConcurrencyLimitServerInterceptor.newBuilder(
        new GrpcServerLimiterBuilder()
            .partitionByHeader(GROUP_HEADER)
            .partition("live", 0.9)
            .partition("batch", 0.1)
            .limit(WindowedLimit.newBuilder()
                    .build(Gradient2Limit.newBuilder()
                            .build()))
            .build();

    ));
```

### Client limiter

There are two main use cases for client side limiters. A client side limiter can protect the client service from its dependent services by failing fast and serving a degraded experience to its client instead of having its latency go up and its resources eventually exhausted. For batch applications that call other services a client side limiter acts as a backpressure mechanism ensuring that the batch application does not put unnecessary load on dependent services.  

In this example a GRPC client will use a blocking version of the VegasLimit to block the caller when the limit has been reached.  

```java
// Create and configure a channel builder
ChannelBuilder builder = ...;

// Add the concurrency limit interceptor
builder.intercept(
    new ConcurrencyLimitClientInterceptor(
        new GrpcClientLimiterBuilder()
            .blockOnLimit(true)
            .build()
        )
    );
```

## Servlet Filter

The purpose of the servlet filter limiter is to protect the servlet from either increased client traffic (batch apps or retry storms) or latency spikes from a dependent service.  With the limiter installed the server can ensure that latencies remain low by rejecting excess traffic with HTTP 429 Too Many Requests errors.

In this example a servlet is configured with a single adaptive limiter that is shared among batch and live traffic with live traffic guaranteed 90% of throughput and 10% guaranteed to batch.  The limiter is given a lookup function that translates the request's Principal to one of the two groups (live vs batch). 

```java
Map<String, String> principalToGroup = ...;
Filter filter = new ConcurrencyLimitServletFilter(new ServletLimiterBuilder()
    .partitionByUserPrincipal(principal -> principalToGroup.get(principal.getName())
    .partition("live", 0.9)
    .partition("batch", 0.1))
    .build());
```

## Executor

The BlockingAdaptiveExecutor adapts the size of an internal thread pool to match the concurrency limit based on measured latencies of Runnable commands and will block when the limit has been reached.
 
```java
public void drainQueue(Queue<Runnable> tasks) {
    Executor executor = new BlockingAdaptiveExecutor(
        SimpleLimiter.newBuilder()
            .build());
    
    while (true) {
        executor.execute(tasks.take());
    }
}

```

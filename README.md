# Overview

Java Library that implements and integrates concepts from TCP congestion control to auto-detect concurrency limits to achieve optimal throughput with optimal latency.

# Background

When thinking of service availability operators traditionally think in terms of RPS. Stress tests are normally performed to determine the RPS at which point the service tips over. RPS limits are then set somewhere below this tipping point (say 75% of this value) and enforced via a token bucket. However, in large distributed systems that auto-scale this value quickly goes out of date and the service falls over anyway and becomes non-responsive to a point where it is unable to gracefully shed load. Instead of thinking in terms of RPS, we should be thinking in terms of concurrent request where we apply queuing theory to determine the number of concurrent requests a service can handle before a queue starts to build up, latencies increase and the service eventually exhausts a hard limit such as CPU, memory, disk or network. This relationship is covered very nicely with Little's Law where `Limit = Average RPS * Average Latency`.

Concurrency limits are very easy to enforce but difficult to determine as they would require operators to fully understand the hardware services run on and coordinate how they scale. Instead we'd prefer to measure or estimate the concurrency limits at each point in the network.  As systems scale and hit limits each node will adjust and enforce its local view of the limit. To estimate the limit we borrow from common TCP congestion control algorithms by equating a system's concurrency limit to a TCP congestion window. 

Before applying the algorithm we need to set some ground rules. 
* We accept that every system has an inherent concurrency limit that is determined by a hard resources, such as number of CPU cores. 
* We accept that this limit can change as a system auto-scales.  
* For large systems it's impossible to know all the hard resource limits so we'd rather measure and estimate that limit.
* We use minimum latency measurements to determine when queuing happens.
* We use timeouts and rejected requests to aggressively back off.

A quick note on using minimum latency. Not all requests are the same and can have varying latency distributions but do tend to converge on an average. But we're not trying to measure average latency. We're trying to detect queuing using the minimum observed latency. That is, regardless of how long it takes to process a request, if there is queuing even the fastest requests to process will sit in the queue and the overall observed latency in a sampling window, especially the minimum observed latency, will increase. When the queue is small the limit can be increased. When the queue grows the limit is decreased.

# Limit algorithms

## Vegas

Delay based algorithm where the bottleneck queue is estimated as 

    `L * (1 - minRTT/sampleRtt)`
    
At the end of each sampling window the limit is increased by 1 if the queue is less than alpha (typically a value between 2-3) or decreased by 1 if the queue is greater than beta (typically a value between 4-6 requests)

# Enforcement Strategies

## Simple

In the simplest use case we don't want to differentiate between requests and so enforce a single gauge of the number of inflight requests.  Requests are rejected immediately once the gauge value equals the limit.

## Percentage

For more complex systems it's desirable to provide certain quality of service guarantees while still making efficient use of resources.  Here we guarantee specific types of requests get a certain percentage of the concurrency limit.  For example, a system that takes both live and batch traffic may want to give live traffic 100% of the limit during heavy load and is OK with starving batch traffic. Or, a system may want to guarantee that 50% of the limit is given to write traffic so writes are never starved.

# Integrations

## Executor

The BlockingAdaptiveExecutor adapts the size of an internal thread pool to match the concurrency limit based on measured latencies of Runnable commands and will block when the limit has been reached.
 
```java
public void drainQueue(Queue<Runnable> tasks) {
    Executor executor = new BlockingAdaptiveExecutor(
        new DefaultLimiter(VegasLimit.newDefault(), new SimpleStrategy()));
    
    while (true) {
        executor.execute(queue.take());
    }
}

```

## GRPC

Coming soon...

## Servlet Filter

Coming soon...

# LoadControl design

LoadControl is a set of overload-control pieces for Go services, packaged as
gRPC-Go interceptors and net/http middleware, plus the harness that measures
them on a public microservice benchmark. Every algorithm here is published.
The work in this repo is the implementations, the way they are combined, the
benchmark patch, the reproduction of a metastable failure, the simulator and
the measurements.

## The problem

A service is overloaded when it receives more work than it can finish. What
happens next decides whether the overload is a blip or an outage:

1. **Queues grow and everything gets slow.** A Go gRPC server starts a
   goroutine for every request, so it never refuses work on its own. Past
   capacity, every request shares the CPU with every other one, latency rises
   for all of them, and they start missing their deadlines together.
   Throughput can stay high while **goodput**, the work finished in time to be
   useful, falls toward zero.
2. **Callers time out and retry.** Each retry is more work. A chain of `n`
   services that each retry `r` times can send `r^n` requests to the bottom
   one for one user action.
3. **The system stays down after the cause is gone.** If the retries alone
   keep load above capacity, removing the trigger (a cache flush, a slow
   database) does not end the outage. This is a **metastable failure**:
   two stable states at the same offered load, and the bad one sustains
   itself. Bronson et al. named it; Huang et al. found it behind many
   published production incidents.

## Pieces

All pieces are optional and switched independently (`loadcontrol.Env`
reads `LC_*` variables), so experiments can isolate them.

### Server side (`loadcontrol.Server`, in order per request)

| Step | What it does | Package |
|---|---|---|
| Deadline check | A request whose deadline has passed, or has less than `MinBudget` left, is rejected before any work. A request that waited in the limiter queue past its deadline is dropped too. | root |
| Static rate limit | Token bucket. Baseline only. | `ratelimit` |
| DAGOR admission level | Compound priority (tier, user hash). Requests above the admission level are shed. The level moves once per window from the mean queueing delay. | `priority` |
| Concurrency limiter | Refuses requests once in-flight work reaches the adaptive limit. Each tier may use only a share of the limit, so the lowest tier is refused first. Optional short wait queue served highest tier first. | `limit` |

Rejections are fast and cheap: `RESOURCE_EXHAUSTED` (gRPC) or 503 (HTTP)
with an `x-lc-shed` reason and optional server pushback.

### Client side (`loadcontrol.Client`)

| Piece | What it does | Package |
|---|---|---|
| Adaptive throttle | Rejects locally with probability `max(0, (requests - K*accepts)/(requests+1))` over a trailing window. | `throttle` |
| Retry budget | gRFC A6 token bucket: failures cost 1 token, successes earn `tokenRatio`, retries only while tokens > max/2. A ratio budget (retries as a fraction of requests) is included for comparison. | `retry` |
| Server pushback | `grpc-retry-pushback-ms`: wait the given time, or do not retry if negative. | `retry` |
| Retry at one layer | A server whose handler failed because one of its own downstream calls failed for good marks the response `x-lc-no-retry`. Callers honoring it do not retry. Only the layer next to the failure retries. | root, `lcgrpc`, `lchttp` |
| Priority propagation | Tier and user priority ride in metadata to every hop. | `lcgrpc`, `lchttp` |

Deadlines propagate for free in gRPC (`grpc-timeout`). For HTTP the
middleware reads and the transport writes `X-Lc-Deadline-Ms`, the remaining
budget in milliseconds.

## The limit algorithms

All three take one sample per finished request: its latency, the in-flight
count when it started, and whether it was dropped (timed out or failed from
overload). They are ports of the published algorithms as implemented in
Netflix's concurrency-limits library, with the same constants. Differences
are listed below.

- **AIMD.** +1 while at least half the limit is in use; multiply by 0.9 on a
  drop or a latency above a timeout. Simple and robust, but it only reacts to
  loss, so it lets latency rise until the timeout before backing off.
- **Vegas.** Estimates the queue the way TCP Vegas estimates packets queued
  in the network: `queue = limit * (1 - rtt_noload / rtt)`. Grows the limit
  while the queue is below `alpha = 3 log10(limit)`, shrinks it above
  `beta = 6 log10(limit)`. `rtt_noload` is the minimum latency seen,
  re-probed every `30 * limit` samples because a minimum learned under one
  workload goes stale.
- **Gradient2.** Compares the latest latency to a long exponential average
  (600 samples): `gradient = clamp(1.5 * long / short, 0.5, 1)`,
  `limit = limit * gradient + 4`, smoothed by 0.2. It does not need a true
  minimum latency. After a long overload, when the long average is more than
  twice the short one, it decays the long average so the limit can recover.

Differences from the reference: limits are float64 internally and floored to
an int when read; bounds default to [4, 500] for the benchmark instead of
[20, 200] (the benchmark's services run at concurrency far below 20); the
Vegas probe uses `math/rand/v2` for jitter. Everything else follows the
reference update rules.

## Tier shares versus DAGOR

Two ways to shed by priority are implemented because they answer different
questions.

- **Tier shares** act inside the concurrency limiter. With shares
  `1, 0.9, 0.7`, sheddable requests are refused once 70% of the limit is in
  flight, default ones at 90%, critical ones only at 100%. Reaction is per
  request and immediate.
- **DAGOR** (Zhou et al., SoCC 2018) keeps an admission level over a
  compound key `(tier, user priority)` with 128 user levels from a hash of
  the user id. Once per second (or 2,000 requests) it moves the level so
  that about 5% fewer (overloaded) or 1% more (not overloaded) of the
  window's requests would be admitted, using a histogram of arrivals by key.
  Because the key includes the user, a user who gets in tends to get all of
  their session's calls in, which matters when one action takes several
  calls. DAGOR's overload signal is queueing delay. A Go gRPC server has no
  request queue, so two signals are offered: the wait time in the limiter's
  queue, and the Go scheduler's own run-queue latency from
  `runtime/metrics` (`/sched/latencies:seconds`), which measures how long
  runnable goroutines wait for one of the process's Ps.

  The second signal turned out to be the wrong one for this deployment, and
  the measurement says so (exp2, `dagor-sched`): with many processes sharing
  two vCPUs, the contention is between processes, inside the kernel's
  scheduler. A goroutine that holds a P but whose OS thread is waiting for
  the CPU counts as running, so Go's run queue stays short while every
  request is slow, and the level never moves. It would fit a server that
  owns its cores and is saturated by its own goroutines.

## Why the one-layer rule works

Retries are only useful where the failure is: the layer right in front of
the failing service knows whether its call failed and can retry against
another replica. Every layer above sees a failure that already had its
retries. If those layers retry too, attempts multiply. The marker is set by
the server-side interceptor when (a) its handler returned an error and
(b) one of the handler's outbound calls had failed after the client side
gave up. The client side records (b) in a per-request flag carried in the
context, so the application code in between does not need to know about it.

## The benchmark and the patch

DeathStarBench hotelReservation (Gan et al., ASPLOS 2019), pinned at commit
`6ecb097`: a Go frontend over HTTP, gRPC services (search, geo, rate,
profile, recommendation, user, reservation), memcached and MongoDB, Consul for
discovery, Jaeger for traces. `bench/hotel/apply_patch.sh` adds one package
(`lcwire`) and changes three places: the server interceptor in each service
becomes a chain (tracing, then LoadControl), the shared dialer chains the
LoadControl client interceptor, and the frontend's mux is wrapped in the HTTP
middleware. The generated diff is `bench/hotel/loadcontrol.patch`.
Application code is not changed. Faults are injected with `tc netem` on
the cache and database containers, cache flushes, and a CPU-hogging
container.

Load is open loop. `cmd/loadgen` sends the benchmark's own request mix (from
its wrk2 script) on a fixed schedule and measures latency from each request's
intended start time, as wrk2 does, so a slow server cannot slow the load down
and hide queueing (coordinated omission). It adds tiers, a user pool and
user-level retries, which the wrk2 script does not have.

## Simulator

`sim` is a discrete-event model of the same topology on a shared
processor-sharing CPU. It runs the library's own policy code (the limit
algorithms, DAGOR, the throttle, the retry budget, the token bucket) on a
virtual clock, and re-implements only the waiting mechanics as events. It is
calibrated from measured runs and checked against them; see `sim/README.md`
and `NUMBERS.md`.

## Sources and credits

- Bronson, Aghayev, Charapko, Zhu. "Metastable Failures in Distributed
  Systems." HotOS 2021.
- Huang, Magnusson, Kannan, et al. "Metastable Failures in the Wild."
  OSDI 2022.
- Beyer, Jones, Petoff, Murphy (eds.). *Site Reliability Engineering*,
  chapter 21 "Handling Overload" (client-side adaptive throttling,
  criticality) and chapter 22 "Addressing Cascading Failures" (deadline
  propagation, retry budgets, dead work).
- Yanacek. "Using load shedding to avoid overload." Amazon Builders'
  Library.
- Netflix concurrency-limits (github.com/Netflix/concurrency-limits, Apache
  2.0): AIMD, Vegas and Gradient2 limits; the ports here follow its update
  rules and constants.
- gRPC proposal A6, "gRPC Retry Design" (retry policy, retry throttling token
  bucket, server pushback).
- Zhou, Xie, Shi, et al. "Overload Control for Scaling WeChat
  Microservices" (DAGOR). SoCC 2018.
- Cho, Mukherjee, Fried, Belay, et al. "Overload Control for µs-scale RPCs
  with Breakwater." OSDI 2020 (credit-based admission and queueing delay as
  the overload signal; the scheduler-latency signal here follows its idea).
- Gan, Zhang, Cheng, et al. "An Open-Source Benchmark Suite for
  Microservices and Their Hardware-Software Implications for Cloud and Edge
  Systems" (DeathStarBench). ASPLOS 2019.
- Tene. "How NOT to Measure Latency" and wrk2 (coordinated omission).
- Brakmo, Peterson. "TCP Vegas." IEEE JSAC 1995.

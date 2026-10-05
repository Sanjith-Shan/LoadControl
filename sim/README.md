# sim: a discrete-event model of hotelReservation under overload

`sim` simulates the DeathStarBench hotelReservation services running on one
2-vCPU VM, with LoadControl's own policy code making every admission, retry
and throttling decision on a virtual clock. It exists to sweep parameters in
seconds instead of hours, and it is only trusted where it has been checked
against measured runs (see Calibration).

```
go run ./cmd/lcsim run   -config load.x=3,LIMIT=gradient2,DEADLINE=on -out results/sim_g2_3x.jsonl
go run ./cmd/lcsim sweep -param load.x -values 0.5,1,1.5,2,3,4 -config LIMIT=gradient2 -out sweep.jsonl
go run ./cmd/lcsim sweep -configs "LIMIT=none;LIMIT=aimd;LIMIT=vegas;LIMIT=gradient2" -config load.x=3 -seeds 3 -out algs.jsonl
go run ./cmd/lcsim calibrate -capacity 1450 -lat search=4.1,recommend=2.6,user=1.2,reserve=3.0 -out sim/params.calibrated.json
```

A run is a pure function of its params and seed. 60 simulated seconds at
5,000 req/s take 1.3 to 4.7 s of wall time on the mini PC (Ryzen 3 4300U),
depending on how much retry traffic the config generates.

## What is library code and what is modeled

Called directly, unmodified, with the virtual clock injected:

| Piece | Library code |
|---|---|
| Limit algorithms | `limit.AIMD`, `limit.Vegas`, `limit.Gradient2`, `limit.Fixed` (`Update`/`Limit`) |
| Priority admission | `priority.Dagor` (`Admit`, `ObserveDelay`, `Tick`), `priority.UserPriority` |
| Client throttle | `throttle.Throttle` (`RejectProbability`, `Allow`, `Accepted`) |
| Retry budgets | `retry.Budget`, `retry.RatioBudget`, `retry.Unlimited`, `retry.Pushback` |
| Rate limit | `ratelimit.Bucket` |
| Configuration | `loadcontrol.Env.ServerConfig` / `ClientConfig` read the same keys as a real deployment |

Re-expressed as events, because the real code blocks goroutines:

- `limiter.go`: `limit.Limiter`'s admission and wait queue. Tier `t` is
  admitted directly only if `inflight < max(1, int(share[t]*limit))` and no
  same-or-higher tier waiter is queued; waiters are ordered by tier then
  FIFO, bounded by `MaxQueue` (default `4*limit`), leave after `MaxWait` or
  when their context ends, and are granted on release. Tokens feed the
  algorithm `Update(rtt, inflight-at-admission, dropped)` exactly as
  `Success`/`Dropped`/`Ignore` do. `TestLimiterMatchesLibrary` checks it
  decision by decision against `limit.Limiter` on random non-blocking
  sequences; `TestLimiterQueue` covers queue order, MaxWait and cancel.
- `model.go` `admit`: `Server.Admit`'s order (default timeout, deadline
  check with `MinBudget`, rate limit, DAGOR, limiter; a request whose
  deadline passed while queued is dropped; DAGOR gets the queue wait).
- `model.go` `attempt`/`onReply`: `Client.Do` with lcgrpc's `classify`:
  throttle check per attempt, `OnResult`/`AllowRetry`, max attempts,
  per-try timeout, backoff, pushback, and the one-layer marker (a server
  whose handler failed after one of its own calls failed for good marks
  the failure no-retry; callers with `ONE_LAYER=on` do not retry it).
- Outcome mapping: lcgrpc's `outcome()` for gRPC services, lchttp's status
  mapping for the HTTP frontend.

Three small departures, all for reproducibility: the throttle's accept draw
and the backoff jitter use the seeded source with the library's formulas
(`Allow` is still called for its bookkeeping), and `limit.Vegas`'s probe
jitter is pointed at the seeded source by reflection (`seedRand`), because
the library draws those from the global generator.

## The model

- **Topology** (from the frontend code): search = frontend, then search
  (geo, then rate), then reservation.CheckAvailability, then profile, all
  sequential; recommend = recommendation then profile; login = user;
  reserve = user then reservation.MakeReservation. Mix 60 / 39 / 0.5 / 0.5.
- **CPU**: one processor-sharing CPU with `cores` cores that every service,
  memcached and MongoDB share (`cpu=fcfs` switches to first come first
  served). Every RPC costs `rpc_server_ms` at the callee before admission
  (so shed requests are not free) and `rpc_client_ms` at the caller; every
  handler has its own mean (`handlers`), drawn exponential, lognormal or
  constant. Without a limiter a service runs a goroutine per request, so
  in-flight work is unbounded and processor sharing slows everything:
  that is the collapse.
- **Caches**: rate, profile and reservation read `per_req` keys from
  memcached; misses go to MongoDB and are filled after the query returns,
  only if the handler is still waiting. `flush` empties the caches and
  drops fills for `dur` seconds (the cold-cache trigger).
- **MongoDB**: a per-service connection pool (`mongo.pool`), non-CPU
  latency (`io_ms`, plus `slow.extra_ms` while the toxiproxy-style `slow`
  trigger is on), then CPU on the shared pool. A request context abandons
  the wait but mongod finishes the work; services in `mongo.ctx_todo` (rate,
  which uses `context.TODO()`) wait it out.
- **Cancellation and dead work**: with `cancel_propagation` (gRPC and an HTTP
  disconnect both do this) a caller giving up cancels the callee: queued
  waiters leave and later calls fail fast, but CPU already started runs to
  completion. With it off, abandoned requests keep running until their own
  deadline (if any). `DEADLINE=on` drops expired requests at admission and
  after queueing.
- **Users**: open loop (`poisson` or `constant`), rate `load.rps` or
  `load.x` times `capacity_rps`, or a schedule `load.steps=0:0.7;30:3`.
  Each request gets a tier from `tiers` and a user from a pool of
  `user.users`. The client has `user.timeout_ms` per attempt and retries
  timeouts and 5xx up to `user.retries` times, with optional backoff and
  optional honoring of the no-retry marker and pushback.

## Configuration

Upper-case keys are LoadControl env keys with the library's semantics and
defaults (`LC_` prefix optional, `<SERVICE>_<KEY>` for one service):
`LIMIT=none|fixed:N|aimd|vegas|gradient2`, `LIMIT_INITIAL/MIN/MAX`,
`QUEUE_WAIT_MS`, `TIER_SHARES=1,0.9,0.7`, `DAGOR=off|wait|sched`,
`DEADLINE=on`, `MIN_BUDGET_MS`, `DEFAULT_TIMEOUT_MS`, `RATELIMIT`,
`PUSHBACK_MS`, `ONE_LAYER=on`, `RETRY=none|naive|budget|ratio`,
`RETRY_ATTEMPTS`, `PER_TRY_TIMEOUT_MS`, `BACKOFF_MS`, `THROTTLE=K`.
A real run's env can be replayed by putting it in `env` or in `-config`.
`DAGOR=sched` uses a stand-in for Go scheduler latency:
`(cpu jobs / cores - 1) * sched_quantum_ms`.

Lower-case keys are dotted paths into the params (`user.retries=2`,
`handlers.geo.nearby=0.3`, `slow.extra_ms=200`, `max_concurrency.search=50`).

## Output

JSON lines. Per simulated second: `offered`, `completed`, `success`,
`goodput` (successful within `slo_ms` of the first attempt, once per user
request however many retries it took), `failed`, `timeout`,
`success_rate`, per tier `success_rate`/`p50_ms`/`p99_ms`, `shed` by
service and reason, `attempts` per service `[original, retry]` as the
server saw them, `user_attempts`, `leaf_per_req` (requests reaching geo,
rate, reservation, profile, recommendation and user per user request),
`mongo_ops`, `cache_hit`, `client_local` (throttle, budget, no_retry
refusals), and per service `limit`, `inflight`, `queue`, plus `cpu_util`.
Each run ends with a `"summary": true` line over `[warmup_s, duration_s)`,
with per-type latencies, `recovery_s` (seconds after the last trigger ends
until goodput is back to 90% of its pre-trigger mean for 5 s; absent if it
never is) and `outcomes` (offered, success, failed, timeout over the whole
run, which always add up).

## Calibration

**Every number in `params.json` is a placeholder.** `capacity_rps` (1600) is
the placeholder model's own simulated peak, not a measurement.

Inputs needed from real runs, all with no LoadControl, the same VM and the
same load generator:

1. **Capacity**: the highest goodput (rps within the SLO) over a sweep of
   offered loads. Required.
2. **Low-load mean latency per request type** (search, recommend, user,
   reserve) at a light rate, e.g. 50 rps. Optional but strongly
   recommended; pass the rate with `-lowload-rps`.
3. Useful checks, not fitted: the goodput curve at 1x to 4x, the frontend
   and per-service CPU split from `docker stats` at a fixed load, cache hit
   rate and MongoDB ops/s, and the recovery time after the cold-cache and
   slowdown triggers.

Procedure (`lcsim calibrate`, `calibrate.go`):

1. If latencies are given, simulate each type at the low rate and scale the
   handlers on its path by the measured/simulated ratio (a handler shared by
   several types moves by the geometric mean of their ratios); repeat until
   every type is within 2%.
2. Find the simulated peak goodput by bisecting for the largest offered load
   still served at 95% goodput. With latencies fitted, the remaining gap is
   CPU off the critical path (kernel networking, GC, tracing, the load
   generator if it shares the VM) and goes into `hidden_ms`; without
   latencies every CPU cost is scaled by one factor. Repeat until within 2%.
3. If the latencies imply more CPU than the capacity allows, the CPU is
   scaled down to fit capacity and the rest of the latency is fitted as
   non-CPU per-hop time (`net_ms`). The report prints the residuals.

Pass `-config` with the measured run's user timeout, mix and env so the
fit uses the same conditions. Then check the calibrated model on runs it
was not fitted to (the 2x to 4x points, the controlled configurations, the
trigger runs) and report the error.

## What the placeholder model shows

From `go test ./sim -v` and the runs behind it (placeholder parameters, so
these are model behaviors, not results):

- At 3x capacity, no control: goodput 0.00x capacity (processor sharing
  makes every request miss the 1 s timeout). Gradient2 with deadline drop:
  0.94x.
- Metastability at 0.7x with a 10 s cold cache and +100 ms MongoDB
  latency: naive per-hop retries (3 attempts, 300 ms per try) plus user
  retries (3 attempts) stay at zero goodput after the trigger ends;
  budget + one-layer + Gradient2 recover within 1 s.
- What sustains it here is the user retries (3 attempts, no backoff, so 0.7x
  becomes about 2.1x once requests time out). User retries alone never
  recover, with or without a per-hop retry budget; honoring the one-layer
  marker at the user (with `ONE_LAYER=on`) recovers in 1 s even without a
  limiter; Gradient2 on every service with deadline drop recovers at once
  with naive or budgeted per-hop retries alike (user retries on).
  Per-hop naive retries alone recover 15 s (0.7x) and 33 s (0.8x)
  after the trigger and not within 70 s at 0.9x. Under `cpu=fcfs` per-hop
  retries alone never recover at 0.7x. So whether the real system needs
  client retries to stay down depends on how CPU is shared, which
  calibration against a measured recovery has to settle.
- The cold cache is the trigger that matters: the MongoDB slowdown alone
  (instant flush, +100 ms for 10 s) does not tip the system.

# sim: a discrete-event model of hotelReservation under overload

`sim` simulates the DeathStarBench hotelReservation services running on one
2-vCPU VM, with LoadControl's own policy code making every admission, retry
and throttling decision on a virtual clock. It exists to sweep parameters in
seconds instead of hours, and it is only trusted where it has been checked
against measured runs (see Calibration and Validation).

```
go run ./cmd/lcsim run   -config load.x=3,LIMIT=gradient2,DEADLINE=on -out results/sim_g2_3x.jsonl
go run ./cmd/lcsim sweep -param load.x -values 0.5,1,1.5,2,3,4 -config LIMIT=gradient2 -out sweep.jsonl
go run ./cmd/lcsim sweep -configs "LIMIT=none;LIMIT=aimd;LIMIT=vegas;LIMIT=gradient2" -config load.x=3 -seeds 3 -out algs.jsonl
go run ./cmd/lcsim replay -out results/sim_replay.jsonl results/*.jsonl
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
- **CPU**: `cores` cores that every service, memcached and MongoDB share,
  in one of three models. `cpu=procs` (the calibrated default) is
  two-level, like the real VM: every job belongs to a process (each Go
  service with `slots.go` = GOMAXPROCS 2, each memcached with
  `slots.memcached` 4 threads, each mongod with `slots.mongod` 4, and a
  `host` process for `hidden_ms`); a process runs at most that many jobs at
  once and queues the rest FIFO like Go's run queues, with Go's runnext slot
  (the goroutine readied most recently, a new handler or a reply handed to
  its caller, runs next; `no_runnext` turns it off) and preemption after
  `slice_ms` (10) of CPU when others wait; the kernel then shares the cores
  equally between all running jobs, which is processor sharing over the
  running set. `cpu=ps` is one processor-sharing pool over every job;
  `cpu=fcfs` is first come first served. Every RPC costs `rpc_server_ms` at the callee before admission
  (so shed requests are not free) and `rpc_client_ms` at the caller; every
  handler has its own mean (`handlers`), drawn exponential, lognormal or
  constant. Without a limiter a service runs a goroutine per request, so
  in-flight work is unbounded: under `ps` every request slows equally and
  goodput collapses to zero; under `procs` the runnext slot lets the newest
  requests through while old ones starve, so some goodput survives, as
  measured.
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
- **Users**: cmd/loadgen's behavior. Open loop (`constant`, its default, or
  `poisson`), rate `load.rps` or `load.x` times `capacity_rps`, a schedule
  in multiples of capacity (`load.steps=0:0.7;30:3`) or in the loadgen's
  own format (`load.schedule=0s:300,30s:900`), and an optional unreported
  warm-up (`load.warmup_s`, `load.warmup_rps`). Each request gets a tier
  from `tiers` and a user index from a pool of `user.users`, sent as
  X-Lc-User (lchttp uses 0..127 directly and hashes the rest). Each attempt
  has `user.timeout_ms`; timeouts and 5xx are retried up to `user.retries`
  times (shed 503s only with `user.retry_shed`), with exponential
  full-jitter backoff on top of any honored pushback, and optional honoring
  of the no-retry marker.
- **Faults**: `faults` is a timeline in bench/lcbench.py's language,
  `[{"t":30,"fault":"flush:rate"},{"t":31,"fault":"latency:mongo-rate:100"},{"t":50,"fault":"clear"}]`:
  `flush:<rate|profile|reserve>` empties a cache, `latency:<proxy>:<ms>`
  delays every response from `mongo-<service>` or `memc-<cache>`,
  `down:`/`up:` refuse connections (MongoDB errors, memcached misses),
  `clear` ends all. `cpu:` squeezes are not modeled and are listed in the
  replay output.

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
request however many retries it took), `slow`, `user_shed`, `failed`,
`timeout`,
`success_rate`, per tier `success_rate`/`p50_ms`/`p99_ms`, `shed` by
service and reason, `attempts` per service `[original, retry]` as the
server saw them, `user_attempts`, `leaf_per_req` (requests reaching geo,
rate, reservation, profile, recommendation and user per user request),
`mongo_ops`, `cache_hit`, `client_local` (throttle, budget, no_retry
refusals), and per service `limit`, `inflight`, `queue`, plus `cpu_util`.
Each run ends with a `"summary": true` line over `[warmup_s, duration_s)`,
with per-type latencies, `recovery_s` (seconds after the last trigger ends
until goodput is back to 90% of its pre-trigger mean for 5 s; absent if it
never is) and `totals` over the whole run in the loadgen's categories
(offered, good, slow, shed, error, timeout), which always add up.

## Calibration

The default parameters are **`params.calibrated.json`**, fitted to the
measured runs on the mini PC (2-vCPU WSL2 VM, everything including the
load generator on it). `params.json` is kept as the uncalibrated
placeholder (guessed costs, `capacity_rps` 1600 = its own simulated peak);
the behavior tests in `sim_test.go` run on it, and `Placeholder()` returns
it. Pass `-params sim/params.json` to use it from the CLI.

Inputs used (all with no LoadControl and cmd/loadgen defaults: evenly
spaced arrivals, 1 s timeout, no retries, 500 ms SLO):

1. **Capacity 400 req/s**: the highest clean no-control goodput in
   `results/exp0_capacity.jsonl` (400 offered gives 398.2 and 398.6 good/s;
   450 gives 360 and 412; 500 gives 189).
2. **Mean latency per request type at low load.** Only the 300 req/s runs
   record per-type latency, and at 300 req/s it is mostly queueing. The two
   runs also disagree by up to 4x (the 02:26 one had 187 timeouts and is
   disturbed), so the clean 02:32 run's means (search 39.741, recommend
   13.974, login 6.993, reserve 47.643 ms) are scaled by 0.2528, the ratio of
   the overall p50 at 50 req/s (5.333 ms) to that at 300 req/s (21.094 ms),
   giving estimated 50 req/s means of 10.05, 3.53, 1.77 and 12.05 ms.
   Fitting the raw 300 req/s means directly does not work: the model at
   300 req/s (utilization about 0.75) queues far less than the real system,
   so the fit pushes almost 6 ms of fixed delay per hop into the model.
   Replace this input with `results/exp7_overhead_e2e.jsonl` (100 req/s)
   once it exists.
3. **hidden_ms 0.5**: CPU per user attempt outside the service handlers
   (load generator, kernel TCP and HTTP, Docker networking), paid by shed
   attempts too. No-control runs cannot separate it from handler CPU; runs
   that shed can. It was chosen from {0, 0.5, 1.0, 1.5} by replay error on
   `exp1_fixedconc_sweep` and `tuning`, which makes those two experiments
   calibration data, not validation.

```
lcsim calibrate -capacity 400 -hidden-ms 0.5 -lowload-rps 50 \
  -lat search=10.05,recommend=3.53,login=1.77,reserve=12.05 \
  -config load.process=constant,cpu=procs -out sim/params.calibrated.json
```

Procedure (`calibrate.go`):

1. Scale every CPU cost by one factor until the simulated peak goodput with
   no control (bisection for the largest offered load still served at 95%
   goodput) is within 1.5% of the measured capacity. This pins the total
   CPU per user request.
2. Fit the latencies at `-lowload-rps` by damped iterative proportional
   fitting of the handlers on each type's path (a handler shared by several
   types moves by the geometric mean of their ratios). With `hidden_ms`
   free, the CPU the paths do not need becomes `hidden_ms`. With it fixed,
   or if the paths need more than the total, the path total stays pinned,
   the handlers take only the shape, and one per-hop non-CPU time
   (`net_ms`) takes the level by least squares.
3. Re-check the peak.

Result: `net_ms` 0.62 per hop, low-load means within 1% for search,
recommend and reserve and 3% for login, simulated peak 395 req/s. The same procedure
under `cpu=ps` converges to identical costs, so `-config cpu=ps` with this
file is the earlier processor-sharing calibration.

## Validation against measured runs

`lcsim replay` rebuilds every recorded run from its own record: LC_* env,
rate or schedule, duration, fault timeline, and the user flags parsed from
`loadgen_cmd` with cmd/loadgen's defaults. lcbench's 15 s warm-up at
100 req/s is simulated and not reported. It runs with the loadgen's seed
and writes measured against simulated per run: offered, good, slow, shed,
error and timeout per second, and for runs with faults the recovery time
computed exactly as `bench/lcnumbers.py` `recovery()` does, on both series.
One parameter set for every run; nothing is tuned per run.

Contaminated runs are written but flagged (`contaminated`) and left out of
the error statistics, by the rule of `bench/lcnumbers.py` `clean()`: a peer
lock before, during or after the run, a service restart, or Windows CPU at
90% or more just before or after it. `-exclude exp/name[@time-prefix],...`
flags more by hand. To rerun over whatever exists:

```
go build -o build/lcsim.exe ./cmd/lcsim
build/lcsim.exe replay -exclude exp3/off-userretry-0.7x -out results/sim_replay.jsonl results/*.jsonl
```

`results/sim_replay*` inputs are skipped. The error table goes to stderr.
`exp3/off-userretry-0.7x` passes the host rule (86.9% before) but was
already broken at second 0 (p50 657 ms, Windows at 87%), so it is
excluded by hand until it is rerun. Excluded now: that run, the exp0
off-400 run with a peer lock, and fixedconc64/128 (host at 100%).

Goodput error per clean run, `results/sim_replay.jsonl`, 2026-10-05,
before (`cpu=ps`) and after (`cpu=procs`, the default) the two-level CPU
model, same calibration procedure and inputs. Relative error only where
measured goodput is at least 5 req/s.

| experiment | clean runs | median abs err good/s, ps / procs | median rel, ps / procs | max rel, ps / procs | within 10%, ps / procs | role |
|---|---|---|---|---|---|---|
| exp0 no control | 18 | 7.5 / 2.9 | 2% / 1% | 91% / 75% | 13 / 15 of 18 | calibration |
| exp1 goodput 1x to 3x | 18 | 52.2 / 45.6 | 18% / 13% | 92% / 79% | 4 / 5 of 17 | validation |
| exp1sweep fixed limit at 2x | 5 | 66.7 / 66.3 | 35% / 34% | 57% / 56% | 1 / 1 of 5 | calibration (hidden_ms) |
| tuning, full config | 6 | 20.4 / 14.1 | 7% / 5% | 17% / 15% | 5 / 5 of 6 | calibration (hidden_ms) |
| exp3 full config, user retries, trigger | 1 | 25.0 / 25.0 | 10% / 10% | 10% / 10% | 1 / 1 | validation |
| all (abs and rel over the 47 with measured >= 5 good/s) | 48 | 32.7 / 22.6 | 9.9% / 8.4% | 92% / 79% | 24 / 27 of 47 | |

The mean absolute error is the same for both (52 good/s): the two-level
model wins where the old one was worst and loses elsewhere.

Better with `procs`:

- **Deep overload with no control.** exp0 at 600 and 800 req/s: 41% and
  91% error under `ps`, 8% and 6% under `procs` (sim 126 and 119 against
  measured 138 and 112 good/s). exp1 off-1.5x and off-2x: 52% and 92%
  under `ps`, 24% and 6% under `procs`. Processor sharing makes every
  request equally late; with per-process run queues and runnext, the
  newest requests get through and old ones starve, which is what the real
  system does (successes with a p50 of about 200 ms while most requests
  time out).

Worse with `procs`:

- **Just past the knee with no control.** exp0 at 450 req/s: sim 104
  against measured 360 and 412 (71% and 75%; `ps` 50% and 56%); 500 req/s:
  43% (`ps` 17%). Both models have a sharp knee at the calibrated 400
  req/s, and the real system degrades gently between 400 and 500.
- **A rate limit at exactly the capacity (exp1 ratelimit, 400 req/s).** At
  1.5x and 2x the real system kept 381 and 385 good/s; the models give 176
  and 153 (`ps`) and 120 and 82 (`procs`). 400 admitted req/s sits on the
  model's knee, so any extra CPU from the rejected half tips it over; the
  real throughput limit is higher than the 400 req/s goodput peak.
- **No control at 3x (exp1 off-3x): measured 2.7, `procs` 87, `ps` 2.9
  good/s.** The two-level model keeps too much goodput when load is far
  above capacity (not in the relative statistics, since measured is under
  5 req/s).

Unchanged, and still the largest errors:

- **Fixed concurrency 2 to 8 and the limiters at 1.5x are 35% to 58% too
  optimistic** (fixedconc-1.5x, gradient2-1.5x, fixedconc2/4/8-2x). Measured
  goodput rises slowly with the limit; in the model a few requests in
  flight saturate the CPU. More `hidden_ms` fixes the fixed-limit sweep
  but breaks the full config at 3x, so no single per-attempt cost fits
  both. Real per-request cost seems to grow with concurrency in a way
  neither CPU model has.
- **exp1 off-1x: measured 313, sim 396 good/s.** The measured run is below
  every exp0 run at the same 400 req/s (394 to 399); run-to-run variance of
  the real system that one parameter set cannot follow.
- **Below capacity with the full config (0.75x) the model sheds nothing;
  the real system sheds 2% to 7%.** Real latency is noisier than
  exponential service times and Gradient2 reads the noise as overload. The
  model will understate false shedding (exp5).

Capacity sensitivity (not adopted, since the procedure takes the measured
400 req/s): calibrating `procs` to 440 or 460 req/s instead moves the knee
toward the real one and fixes the rate-limit runs (460: ratelimit-2x 4%,
exp0 450 9% and 25%) and lowers the mean absolute error from 52 to 47
(440) and 43 (460) good/s, at the cost of the fixed-limit sweep (42% to
45% median). The real throughput limit is above the 400 req/s goodput
peak; a calibration target defined on throughput rather than goodput is
the next thing to try.

## What the placeholder model shows

From `go test ./sim -v` and the runs behind it (placeholder parameters, so
these are model behaviors, not results):
The behavior tests run under both `cpu=ps` and `cpu=procs`. Under `procs`
the same tests pass with one threshold changed: at 3x with no control the
two-level model keeps 0.36x of capacity instead of 0.00x (the measured
system kept 0.28x at 2x), so the test bound for "collapses" moved from 0.3x
to 0.4x. With naive retries after the trigger it settles at 23% of the
pre-trigger goodput instead of 0 and never recovers; the controlled
configuration recovers at once. The bullets below are the `ps` numbers.


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

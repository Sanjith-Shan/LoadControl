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
5,000 req/s take 0.7 to 1.8 s of wall time with the calibrated default
(1.3 to 4.7 s with the placeholder) on the mini PC (Ryzen 3 4300U),
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
- **CPU**: `cores` cores that every service, memcached, MongoDB, the load
  generator and Docker's proxy share, in one of three models. `cpu=procs`
  (the calibrated default) is two-level, like the real VM. Every job
  belongs to a process: each Go service (`slots.go` = GOMAXPROCS 2), each
  memcached (`slots.memcached` 4 threads), each mongod (`slots.mongod` 4),
  docker-proxy (a Go process), and a `host` process for `hidden_ms`. A
  process runs at most that many jobs at once and queues the rest FIFO like
  Go's run queues, with Go's runnext slot (the goroutine readied most
  recently, a new handler or a reply handed to its caller, runs next;
  `no_runnext` turns it off) and preemption after `slice_ms` (10) of CPU
  when others wait. The kernel shares the cores between busy processes by
  weight, as CFS does between cgroups: each process has `weights` (Docker's
  default cpu-shares, 1024), gets cores*w/W but at most one core per
  running thread, and the rest is shared out again (water filling); its
  threads split its share. `cpu=ps` is one processor-sharing pool over every
  job; `cpu=fcfs` is first come first served. Every RPC costs
  `rpc_server_ms` at the callee before admission (so shed requests are not
  free) and `rpc_client_ms` at the caller; every handler has its own mean
  (`handlers`), drawn exponential, lognormal or constant. Without a limiter
  a service runs a goroutine per request, so in-flight work is unbounded:
  under `ps` every request slows equally and goodput collapses to zero;
  under `procs` the runnext slot lets the newest requests through while
  old ones starve, so some goodput survives, as measured.
- **Connections** (`conn`, on in the calibrated file). The load generator
  keeps idle keep-alive connections (up to `conn.max_idle`). A response
  returns its connection to the pool; an attempt that times out closes its
  connection, so the next attempt needs a new one. Every request to the
  frontend's published port passes through docker-proxy (`conn.req_ms` of
  CPU); a new connection also costs `conn.new_ms` there (accept, dial the
  container, start the copying goroutines, tear down) and
  `conn.frontend_new_ms` in the frontend. The kernel completes the client's
  connect at once (listen backlog), so a client that gives up while its new
  connection waits for docker-proxy has still cost docker-proxy the setup,
  and nothing is forwarded. This is the loop that keeps the measured system
  down: timeouts force new connections, new connections cost about 1 ms of
  CPU each in docker-proxy, at 4 attempts per user request that alone is
  more than one core, so everything stays slow and keeps timing out.
- **Caches**, as in the services' code. One memcached GetMulti per cache
  read (no context: it cannot be cancelled); misses go to MongoDB, rate
  and profile one query per missing hotel concurrently, reservation
  capacities one `$in` query; reservation counts only go to MongoDB when
  GetMulti returns ErrCacheMiss, which it does not for partial misses, so
  they cost nothing and are never filled. After the query the handler goes
  on and a goroutine sets the keys (`go MemcClient.Set`). Reservation reads
  two caches on the same memcached (`reserve_cap`, `reserve`). A large
  reply needs several TCP round trips from an idle connection (slow start,
  10 segments doubling); this matters only under injected latency: the rate
  service stores all 28 rate plans under every hotel key
  (`value_bytes` 5300), so a 5-key GetMulti needs 2 round trips. `flush`
  empties the caches of one memcached.
- **MongoDB**: a per-service connection pool (`mongo.pool`), non-CPU
  latency (`io_ms`), the query's CPU on mongod, then any injected latency.
  A request context abandons the wait but mongod finishes the work;
  services in `mongo.ctx_todo` wait it out (all three: every query in
  rate, profile and reservation uses `context.TODO()`).
- **Cancellation and dead work**: with `cancel_propagation` (gRPC and an HTTP
  disconnect both do this) a caller giving up cancels the callee: queued
  waiters leave and later calls fail fast, but CPU already started runs to
  completion, and memcached and MongoDB calls (no context) run on.
  `DEADLINE=on` drops expired requests at admission and after queueing.
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
  `[{"t":30,"fault":"flush:rate"},{"t":31,"fault":"latency:memc-rate:200"},{"t":50,"fault":"clear"}]`:
  `flush:<rate|profile|reserve>` empties a memcached,
  `latency:<target>:<ms>` delays every reply from `mongo-<service>` or
  `memc-<cache>` (toxiproxy or netem; per TCP round trip for memcached),
  `down:`/`up:` refuse connections (MongoDB errors, memcached misses),
  `hog:<n>` and `hogw:<n>` start a busy-loop container with n threads at
  weight 1024 or `hogw_weight` (20480, its --cpu-shares), `clear` ends all.
  Hogs need `cpu=procs`. `cpu:` squeezes are not modeled and are listed in
  the replay output.

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
placeholder (guessed costs, `capacity_rps` 1600 = its own simulated peak,
no connection layer); the behavior tests in `sim_test.go` run on it, and
`Placeholder()` returns it. Pass `-params sim/params.json` to use it from
the CLI.

Fitted inputs (all with no LoadControl and cmd/loadgen defaults: evenly
spaced arrivals, 1 s timeout, no retries, 500 ms SLO):

1. **Capacity 400 req/s**: the highest clean no-control goodput in
   `results/exp0_capacity.jsonl` (400 offered gives 398.2 and 398.6 good/s;
   450 gives 360 and 412; 500 gives 189).
2. **Mean latency per request type at 100 req/s**, from
   `results/exp7_overhead_e2e.jsonl` off-0.25x: search 9.221, recommend
   4.283, login 2.579, reserve 9.467 ms. (The previous calibration used
   estimates scaled from a 300 req/s run: 10.05, 3.53, 1.77, 12.05.)
3. **hidden_ms 1.0**: CPU per user attempt outside the service handlers
   (load generator, kernel TCP and HTTP), paid by shed attempts too.
   No-control runs cannot separate it from handler CPU; runs that shed
   can. It was chosen from {0, 0.5, 1.0} by mean absolute replay error on
   `exp1_fixedconc_sweep` and `tuning`, which makes those two experiments
   calibration data, not validation.

Set from evidence, not fitted to goodput:

- **conn.new_ms 1.0**: docker-proxy CPU per new connection. In the
  collapsed exp3 runs docker-proxy shows 65% to 71% of a core averaged over
  its whole life (about 185 s) after carrying 70k to 130k connections:
  0.95 to 1.75 ms each. The low end is used. In healthy runs it stays
  under 5%, so `conn.req_ms` 0.05.
- **Cache shapes and sizes** from the service code (see The model).

```
lcsim calibrate -capacity 400 -hidden-ms 1.0 -lowload-rps 100 \
  -lat search=9.221,recommend=4.283,login=2.579,reserve=9.467 \
  -config load.process=constant -out sim/params.calibrated.json
```

(run from a params file that already has `cpu`, `conn` and `caches` as in
`params.calibrated.json`; calibration only moves the CPU costs, `net_ms`
and `hidden_ms`.)

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

Result: `net_ms` 0.70 per hop, the four 100 req/s means within 1% (login
6%), simulated peak 403 req/s.

## Validation against measured runs

`lcsim replay` rebuilds every recorded run from its own record: LC_* env,
rate or schedule, duration, fault timeline, and the user flags parsed from
`loadgen_cmd` with cmd/loadgen's defaults. lcbench's 15 s warm-up at
100 req/s is simulated and not reported. It runs with the loadgen's seed
and writes measured against simulated per run: offered, good, slow, shed,
error and timeout per second, and for runs with faults the recovery time
computed exactly as `bench/numbers.py` `recovery()` does, on both series.
One parameter set for every run; nothing is tuned per run.

Contaminated runs are written but flagged (`contaminated`) and left out of
the error statistics, by the rule of the bench scripts' `clean()` (peer
lock, service restart, host saturated before, after or during the run);
`-exclude exp/name[@time-prefix],...` flags more by hand. Kubernetes runs
(`k8s_*`) are skipped by default: they are a different deployment. To rerun
over whatever exists:

```
go build -o build/lcsim.exe ./cmd/lcsim
build/lcsim.exe replay -parallel 4 -exclude exp3/off-userretry-0.7x -out results/sim_replay.jsonl results/*.jsonl
```

`-seconds f.jsonl` also writes each run's simulated per-second series.

### Results

`results/sim_replay.jsonl`, 2026-10-05, 199 runs, 165 clean with measured
goodput of at least 5 req/s. "Before" is the previous model (two-level CPU
with equal shares per thread, no connection layer, one memcached round
trip per read, rate queries in sequence) as the coordinator replayed it
with its calibration; "after" is the current default.

| experiment | clean runs | median rel err, before / after | within 10%, before / after | median abs err good/s, before / after |
|---|---|---|---|---|
| exp0 capacity, no control | 15 | 0.6% / 0.5% | 12 / 11 of 15 | 2.5 / 1.8 |
| exp1 goodput 1x to 4x | 34 | 12.5% / 13.0% | 11 / 13 of 31 | 41.5 / 43.1 |
| exp1sweep fixed limits | 13 | 34% / 38% | 1 / 2 of 13 | 91 / 106 |
| exp2 priority | 27 | 11.3% / 10.9% | 9 / 9 of 23 | 41.2 / 36.8 |
| exp3 metastable | 22 | 6.7% / 2.4% | 13 / 14 of 22 | 16.6 / 5.1 |
| exp4 amplification | 11 | 25% / 25% | 0 / 0 of 11 | 23.9 / 23.9 |
| exp5 false shedding | 19 | 0.7% / 0.7% | 16 / 15 of 19 | 1.4 / 1.4 |
| exp6 algorithms | 13 | 17% / 24% | 3 / 3 of 12 | 42.4 / 58.6 |
| exp7 overhead | 2 | 0% / 0% | 2 / 2 | 0 / 0 |
| exp8 capacity shift (hog) | 8 | 47% / 13% | 2 / 3 of 8 | 114 / 31 |
| tuning | 6 | 4.6% / 7.3% | 5 / 4 of 6 | 14.1 / 20.4 |
| wrk2 cross-check | 3 | 0% / 0% | 3 / 3 | 0 / 0 |
| **all** | 165 | **11.0% / 10.7%** | **77 / 79** | |

Recovery (runs with faults, numbers.py's definition): measured and
simulated agree on "recovers" against "never recovers" in 21 of 37 runs,
before 17. Of the 9 measured never-recovering runs, the model now never
recovers in 6, before in none.

### What the model now captures

- **The metastable failures in exp3.** With users retrying 3 times on a
  1 s timeout at 0.7x, a cold cache plus 200 ms on every memcached reply,
  or a weighted CPU hog, leaves the measured system at zero goodput with
  all 1120 attempts per second timing out for the rest of the run. The
  model now does the same in all four off/naive memcached runs and all
  three non-limited hogw runs, while the full LoadControl configuration
  recovers in both (2 to 3 s, measured 2 s), and the unweighted `hog` does
  not tip anything (measured the same). The mechanism, from the records
  and the code:
  1. Under 200 ms netem a search needs about 1 s: rate's GetMulti takes two
     round trips because every rate value holds all 28 rate plans
     (about 27 KB for 5 hotels, more than the initial congestion window),
     then one each for profile and reservation's two reads. Searches start
     to hit the 1 s client timeout; recommends (one memcached read) still
     succeed, which is the 100 to 120 good/s the real system shows during
     the trigger.
  2. Every attempt that times out closes its keep-alive connection, so
     each retry opens a new one through docker-proxy, Docker's userland
     proxy for the published port. In the collapsed runs the frontend saw
     only 45% of the attempts, and docker-proxy averaged 65% to 71% of a
     core over its life, against under 5% in healthy runs: about 1 ms of
     CPU per connection.
  3. At 4 attempts per user request that is more than one core of
     docker-proxy alone, on a 2-vCPU VM where every container gets an equal
     share. Requests wait behind connection setups, time out, retry on new
     connections: the state sustains itself after the trigger is gone.
     Fast 503s from the limiter keep connections alive, which is why the
     full configuration never enters it.
- **CPU hogs** (`hog`, `hogw`): exp8 went from 47% to 13% median error,
  because a weight-20480 container now takes a core and a weight-1024 one
  takes its share.

### What it still misses

- **Spontaneous collapses.** Two of the four measured no-trigger runs
  (off-notrigger-0.7x and off-notrigger-0.7x-memc) and the cold-cache-only
  naive run, long after its trigger had passed, started degrading at 57 to
  110 s and reached zero goodput at 102 to 136 s; the model never does,
  because nothing in it produces that large a fluctuation (or a slow
  build-up, which the data cannot rule out).
- **Knife-edge cases go the wrong way.** budget-userretry-0.7x-memc
  recovered 8 s after the trigger in the real system (its per-hop 250 ms
  timeouts turned slow searches into fast 500s, which keep connections
  alive); in the model it collapses. The cold-cache-only naive run
  collapses in the model at the trigger, not about 75 s later.
- **The knee is still too sharp.** Just above the calibrated 400 req/s the
  model collapses where the real system degrades slowly: no control at
  450 req/s is 71 against 360 and 412 good/s; a rate limit at exactly
  400 req/s at 1.5x to 3x is 50 to 113 against 340 to 385; exp8's 0.9x with
  an unweighted hog is 201 against 351. The connection layer made this
  worse, because any timeout now costs a reconnection. The real throughput
  limit is above the 400 req/s goodput peak.
- **Fixed concurrency limits at 2x and 3x** are still 33% to 75% off in
  both directions (exp1sweep), and AIMD and Vegas at 3x (exp6) 38% to 50%:
  per-request cost seems to grow with concurrency in a way the model does
  not have.
- **exp4** runs are 25% to 35% low throughout (unchanged).
- **False shedding below capacity** (exp5): the model sheds less than the
  real system, whose latency is noisier.

## What the placeholder model shows

From `go test ./sim -v` and the runs behind it (placeholder parameters, so
these are model behaviors, not results):
The behavior tests run under both `cpu=ps` and `cpu=procs`. Under `procs`
the same tests pass with one threshold changed: at 3x with no control the
two-level model keeps 0.36x of capacity instead of 0.00x (the measured
system kept 0.28x at 2x), so the test bound for "collapses" moved from 0.3x
to 0.4x. With naive retries after the trigger it settles at about 10% of the
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

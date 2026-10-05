# Bug log

## B1: lchttp.Transport response body unreadable when PerTryTimeout is set
- Found by: TestTransportBodyReadable (lchttp), with a backend that flushes headers before the body
- Symptom: with ClientConfig.PerTryTimeout > 0, reading the body of the response returned by Transport.RoundTrip failed with "context canceled" whenever the body had not fully arrived with the headers.
- Cause: Client.Do cancels each attempt's per-try context as soon as the attempt function returns, but the HTTP request was sent on that context, so the cancel tore down the response stream before the caller could read it.
- Fix: Transport sends each attempt on its own context derived from the caller's context with the same deadline as the per-try context, and cancels it when the response body is closed (or the round trip fails).

## B2: benchmark cost drifted run over run because seed data was duplicated
- Found by: per-container CPU check (`bench/scripts/cpu_profile.sh`) during the first capacity sweep, then a document count in the rate database
- Symptom: the same 50 req/s load used 71% of the VM's CPU an hour after it used far less, and the rate service went from a third of the CPU to most of it. Each run's numbers were worse than the one before.
- Cause: hotelReservation inserts its seed data on every service start without checking what is already there. The harness recreates the services before each run and the databases sit on named volumes, so the rate inventory grew by 27 rate plans per restart (648 after 24 restarts). The rate service reads the whole inventory on a cache miss and caches all of it under every hotel id, so its cost per request grew with every run.
- Fix: `bench/scripts/reset_db.sh` drops the benchmark databases before every run, so every run starts from the same seed data. The capacity sweep was thrown away and rerun.

## B3: fault injection through Toxiproxy did nothing
- Found by: exp4 itself. A 400 ms fault on the rate cache produced zero retries and no change in latency, so I timed requests by hand (no change with the toxic on), checked that the toxic worked on a fresh and on an existing connection (it did, 403 ms), and then listed who was connected to memcached: only the rate service, directly, and Toxiproxy only for my test connections.
- Symptom: every latency fault in the first exp3 round and in the first exp4 round was a no-op. Cache flushes (done with `docker exec` on memcached) did work.
- Cause: the benchmark image runs its services from `/workspace` and reads `./config.json`, the copy baked in at build time. Its Compose file (and mine, copied from it) mounts the config at `/config.json`, which nothing reads, so my addresses pointing at Toxiproxy were ignored.
- Fix: the config is mounted at `/workspace/config.json`, Toxiproxy is gone, and faults are injected with `tc netem` in the cache or database container's network namespace (`bench/tc`, `lcbench.py tc()`), which adds no hop and costs nothing outside a fault window, so the no-control baseline did not change. The first exp4 round moved to `results/invalid/`; the first exp3 round is kept as `results/exp3_coldcache_only.jsonl`, since its cache flushes were real and its latency faults were not.

## B4: the benchmark's rate service crashed whenever memcached was slow
- Found by: the first capacity probe; `docker inspect` restart counts and the rate service's log (`panic: Memmcached error ... i/o timeout`).
- Symptom: under load the rate container restarted every few minutes, and each restart looked like an outage.
- Cause: hotelReservation's rate handler calls `log.Panic` when a memcached read fails, and the memcached client times out after 2 s. On a saturated 2-vCPU VM a 2 s cache read happens, so overload turned into a crash loop.
- Fix: no code change to the benchmark. Its own `MEMC_TIMEOUT` knob is set to 10 s in the Compose file, and every result line records restarts per service so a run with a crash can be told apart.

## B5: Prometheus scraped the wrong service after containers were recreated
- Found by: the per-service inbound counters. The `recommendation:9100` target reported `service=srv-rate`, and rate looked like it got twice the calls search made.
- Symptom: counters attributed to the wrong service after a run's restart of the stack.
- Cause: recreating containers reassigns their IPs, and Prometheus keeps HTTP connections to the old IPs, so a target name could reach another container.
- Fix: the harness restarts Prometheus after recreating the services, and reads counters as exact differences of two snapshots instead of `increase()`, which extrapolates.

## B6: the WSL distro shut down between commands and took the stack with it
- Found by: a smoke run whose last seconds were all errors, then `uptime` and the Docker journal showing the daemon had just started again.
- Symptom: every container restarted together in the middle of a run.
- Cause: WSL stops an idle distro shortly after the last `wsl.exe` session exits, and each tool call was its own short session.
- Fix: a long-lived `sleep infinity` session keeps the distro up for the whole build.

## B7: Windows line endings broke the patch script and CI
- Found by: bash (`set: pipefail: invalid option name`) and the CI gofmt step.
- Symptom: `apply_patch.sh` failed in WSL; CI failed on formatting.
- Cause: files edited with Python on Windows were written with CRLF.
- Fix: every file rewritten with LF, `.gitattributes` with `eol=lf`, and edits since then write bytes or `newline="\n"`.

## B8: runs were contaminated by other work on the host
- Found by: comparing the same configuration across runs. Four exp2 runs and two exp1 runs had a mid-run Windows CPU of 77% to 98% (the VM alone shows 55-65%) and goodput far below their repeats.
- Symptom: the 1.5x points of the first exp1 round dipped, and the first exp2 round's adaptive configurations looked much worse than in exp1.
- Cause: the VM's 2 vCPUs share 4 physical cores with Windows. Builds and replays from another project, and anything else on the host, take cores the benchmark was using, and the VM cannot see it (no steal time in WSL).
- Fix: a continuous host CPU sampler (`bench/scripts/hostcpu.ps1`) whose samples each run records, a shared lock with the other project, and a clean-run rule in `bench/lcnumbers.py` and the simulator's replay. Contaminated points were rerun; tables average clean runs only.

## B9: DAGOR on the Go scheduler's latency never shed anything
- Found by: exp2 (`dagor-sched`, 3 runs): 77 good req/s at 3x, about as bad as no control.
- Symptom: the admission level stayed at "admit all" while every request was slow.
- Cause: `/sched/latencies:seconds` measures how long a goroutine waits for one of its process's Ps. On a VM shared by twenty processes, the contention is in the kernel between processes; a goroutine whose OS thread waits for the CPU still counts as running, so the signal stays low. The signal assumed a server that owns its cores.
- Fix: none in code. DESIGN.md explains when the signal is valid; the limiter-queue signal is the one to use here.

## B10: the one-layer rule leaked when timeouts did not nest
- Found by: exp4 (`onelayer`): 4.1 requests reached the rate service per user search instead of the 3 the rule allows.
- Symptom: the frontend still retried search during the fault, although search marked its failures `x-lc-no-retry`.
- Cause: the frontend's per-try timeout (800 ms) was shorter than search's three 250 ms attempts plus overhead. The frontend gave up on its own timer before search's marked failure arrived, and its own timeout is retryable.
- Fix: the rule only holds if each layer's per-try timeout exceeds the total retry time below it, or the caller's retries are budgeted. With the gRFC A6 budget at every hop the count was 1.68 even with users retrying. DESIGN.md states the nesting requirement.

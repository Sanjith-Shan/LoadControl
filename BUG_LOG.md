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

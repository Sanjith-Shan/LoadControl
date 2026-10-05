# Reproducing the measurements

Everything here ran on one mini PC (AMD Ryzen 3 4300U, 4 cores, 16 GB) with
the benchmark inside a WSL2 Ubuntu 24.04 VM limited to 2 vCPUs and 6 GB, and
Docker 29 inside that VM. The load generator runs in the same VM: WSL's
localhost forwarding from Windows could not carry the load, so the generator
competes with the services for the same 2 vCPUs, and every result line
records how busy the VM and the host were.

## 1. Build the patched benchmark image

```
git clone https://github.com/delimitrou/DeathStarBench ~/lcwork/DeathStarBench
git -C ~/lcwork/DeathStarBench checkout 6ecb09706140f8730b5385c08f1386c654c3c526
LC=$PWD bash bench/hotel/build_image.sh      # writes ~/lcwork/hotel and loadcontrol/hotel:latest
```

`apply_patch.sh` copies hotelReservation, adds `lcwire` (from
`bench/hotel/_overlay`), chains the LoadControl interceptors after the tracing
ones, wraps the frontend's mux, and points `go.mod` at this repo. The
resulting diff, without vendored code, is `bench/hotel/loadcontrol.patch`.

## 2. Start the stack

```
cd bench/hotel && docker compose up -d
```

hotelReservation (frontend, search, geo, rate, profile, recommendation, user,
reservation), their memcached and MongoDB, Consul, Jaeger (UI on 16686), Prometheus (9090, 1 s scrapes) and Grafana (3000, dashboard
"LoadControl"). Review and attractions are left out because the benchmark's
mixed workload never calls them.

`MEMC_TIMEOUT=10`: the rate service panics when a memcached read takes longer
than its timeout (2 s by default), which under overload turns into a crash
loop. The benchmark's own knob raises it so crashes do not dominate the
overload behavior. Each result line records container restarts.

## 3. Run

```
go build -o build/loadgen-linux ./cmd/loadgen          # GOOS=linux if building elsewhere
python3 bench/lcbench.py --exp exp1 --name off-3x --env bench/hotel/configs/off.env \
    --rate 1050 --duration 60 --out results/exp1_goodput.jsonl
python3 bench/experiments.py exp1 --cap 350             # a whole experiment
python3 bench/summarize.py results/exp1_goodput.jsonl   # one line per run
```

Per run, `lcbench.py`:

1. takes `/tmp/BENCH_LOCK` (shared with another project on the same machine;
   it waits while the other holds it and yields every 40 minutes),
2. drops the benchmark databases (`scripts/reset_db.sh`: the services seed
   them on every start and would otherwise duplicate the seed data),
3. recreates the eight services with the run's `LC_*` env file, restarts
   Prometheus (it otherwise keeps scraping old container IPs), and waits
   for five healthy requests in a row,
4. flushes the caches and warms them with 15 s at 100 req/s,
5. records the machine and the load (load average, busy processes, other
   containers, the peer's lock, Windows CPU) before, in the middle of and
   after the run, and the VM's CPU use every second,
6. runs `loadgen` open loop and applies the fault timeline (netem
   latency, cache flushes, a CPU hog),
7. snapshots LoadControl's Prometheus counters before and after, and
8. appends one JSON line with all of it to the results file.

## Fault injection

| Spec | Effect |
|---|---|
| `flush:rate` (`profile`, `reserve`) | memcached `flush_all`: the cold-cache trigger |
| `latency:memc-rate:200` | `tc netem` adds 200 ms to everything the rate cache sends (`mongo-rate`, `memc-profile`, ... likewise) |
| `hog:1` | a busy-loop container: a noisy neighbour takes one of the two vCPUs |
| `clear` | remove every fault |

Faults run in the target container's network namespace from a small `tc`
image (`bench/tc`), so nothing sits in the data path outside a fault window.
(An earlier version used Toxiproxy, which the benchmark never actually
connected through; see BUG_LOG B3.)

## Kubernetes

`bench/k8s/run_k3d.sh up` creates a one-node k3d cluster, imports the
images and deploys `bench/k8s/hotel.yaml`, which `gen.py` generates from the
Compose setup (same images, config and proxies). Experiments then run with
`LC_PLATFORM=k8s KUBECTL=~/lcwork/bin/kubectl`.

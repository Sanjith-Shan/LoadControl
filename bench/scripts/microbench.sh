#!/usr/bin/env bash
# exp7: middleware overhead as Go benchmarks, run on the benchmark VM while
# holding the bench lock (nothing else running). Writes results/exp7_microbench.jsonl.
set -e
cd "$(dirname "$0")/../.."
GO=${GO:-$HOME/lcwork/go/bin/go}
export GOCACHE=/tmp/lc-gocache GOMODCACHE=$HOME/lcwork/gomodcache GOFLAGS=-mod=mod
raw=$($GO test -run '^$' -bench . -benchtime=2s -count=6 -benchmem . ./limit ./lcgrpc 2>&1)
python3 - "$raw" <<'PY'
import json, re, sys, time, os, statistics as st
raw = sys.argv[1]
rows = {}
for line in raw.splitlines():
    m = re.match(r'^(Benchmark\S+)-(\d+)\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op', line)
    if m:
        rows.setdefault(m.group(1), []).append((float(m.group(4)), int(m.group(5)), int(m.group(6))))
cpu = [l.split(':', 1)[1].strip() for l in open('/proc/cpuinfo') if l.startswith('model name')][0]
rec = {"exp": "exp7", "name": "microbench", "time": time.strftime('%Y-%m-%dT%H:%M:%S%z'),
       "machine": {"cpu": cpu, "vcpus": os.cpu_count(), "vm": "WSL2 Ubuntu-24.04", "go": os.popen(os.environ.get('GO', 'go') + ' version').read().strip()},
       "load": {"loadavg_after": open('/proc/loadavg').read().split()[:3]},
       "benchmarks": {k: {"ns_op_median": st.median(x[0] for x in v), "ns_op_all": [x[0] for x in v],
                          "bytes_op": v[0][1], "allocs_op": v[0][2]} for k, v in rows.items()},
       "raw": raw[-6000:]}
open("results/exp7_microbench.jsonl", "a").write(json.dumps(rec) + "\n")
for k, v in rec["benchmarks"].items():
    print(f"{k:45} {v['ns_op_median']:12.1f} ns/op {v['bytes_op']:6} B/op {v['allocs_op']:4} allocs/op")
PY

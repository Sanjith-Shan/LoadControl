#!/usr/bin/env bash
# Cross-checks cmd/loadgen against the benchmark's own wrk2 and Lua script:
# the same no-control stack driven at the same rates by each tool. Writes
# results/wrk2_crosscheck.jsonl (wrk2 lines) next to loadgen runs made by
# lcbench.py with the same rates.
set -e
cd "$(dirname "$0")/../.."
OUT=results/wrk2_crosscheck.jsonl
docker image inspect loadcontrol/wrk2 >/dev/null 2>&1 || bash bench/scripts/with_lock.sh bash bench/scripts/build_wrk2.sh
for r in ${RATES:-200 300 400}; do
  # loadgen run through the harness (same restart, warmup and recording)
  python3 bench/lcbench.py --exp wrk2check --name loadgen-$r --env bench/hotel/configs/off.env --rate $r --duration 60 --out $OUT
  # wrk2 run: same restart and warmup, then wrk2 instead of loadgen
  bash bench/scripts/with_lock.sh bash -c "
    python3 bench/lcbench.py --exp wrk2check-prep --name prep --env bench/hotel/configs/off.env --rate 1 --duration 1 --out /tmp/prep.jsonl >/dev/null
    docker run --rm --network host loadcontrol/wrk2 -D exp -t2 -c64 -d60s -L -s /scripts/hotel-reservation/mixed-workload_type_1.lua http://localhost:5000 -R$r > /tmp/wrk2_$r.txt 2>&1 || true
    python3 - <<PY
import json, re, time
t = open('/tmp/wrk2_$r.txt').read()
def lat(p):
    m = re.search(r'^\s*' + re.escape(p) + r'%\s+([\d.]+)(us|ms|s)', t, re.M)
    if not m: return None
    v, u = float(m.group(1)), m.group(2)
    return v / 1000 if u == 'us' else v * 1000 if u == 's' else v
req = re.search(r'Requests/sec:\s+([\d.]+)', t)
non2xx = re.search(r'Non-2xx or 3xx responses:\s+(\d+)', t)
to = re.search(r'timeout (\d+)', t)
rec = {'exp': 'wrk2check', 'name': 'wrk2-$r', 'time': time.strftime('%Y-%m-%dT%H:%M:%S%z'), 'rate': $r,
       'requests_per_s': float(req.group(1)) if req else None, 'p50_ms': lat('50.000'), 'p99_ms': lat('99.000'),
       'non2xx': int(non2xx.group(1)) if non2xx else 0, 'socket_timeouts': int(to.group(1)) if to else 0,
       'loadavg': open('/proc/loadavg').read().split()[:3], 'raw': t[-3000:]}
open('$OUT', 'a').write(json.dumps(rec) + '\n')
print('wrk2', $r, rec['requests_per_s'], rec['p50_ms'], rec['p99_ms'])
PY"
done

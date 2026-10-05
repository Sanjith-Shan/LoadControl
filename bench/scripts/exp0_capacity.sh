#!/usr/bin/env bash
# M0: capacity curve with no control. Goodput vs offered load.
set -e
cd "$(dirname "$0")/../.."
out=${OUT:-results/exp0_capacity.jsonl}
for r in ${RATES:-25 50 75 100 125 150 175 200 250 300 400 500}; do
  python3 bench/lcbench.py --exp exp0 --name off-$r --env bench/hotel/configs/off.env \
    --rate $r --duration 60 --out "$out" 2>&1 | tail -1
done

#!/usr/bin/env bash
# Quick capacity probe with no control: probe.sh <out> <rate>...
set -e
cd "$(dirname "$0")/../.."
out=$1; shift
for r in "$@"; do
  python3 bench/lcbench.py --exp probe --name off-$r --env bench/hotel/configs/off.env --rate $r --duration 30 --warmup 10 --out "$out" 2>&1 | tail -1
done

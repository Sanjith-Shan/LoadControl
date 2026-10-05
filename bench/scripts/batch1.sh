#!/usr/bin/env bash
# Peak region of the capacity curve, repeated, then the static concurrency sweep.
cd "$(dirname "$0")/../.."
for rep in 1 2; do
  RATES="300 350 400 450" bash bench/scripts/exp0_capacity.sh
done
python3 bench/experiments.py exp_conc_sweep --cap 350

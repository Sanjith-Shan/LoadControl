#!/usr/bin/env bash
# Second round: stronger metastability triggers, capacity shift, clean reruns
# of contaminated points, repeats of the headline points, cross-checks.
cd "$(dirname "$0")/../.."
E="python3 bench/experiments.py"
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200
$E exp3 --cap 400 --conc 32 --trigger hog
$E exp8 --cap 400 --conc 32
$E exp_conc_sweep --cap 350 --ns 48,64,128
$E exp1 --cap 400 --conc 32 --loads 3 --only off,fixedconc,gradient2,full --reps 2
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200 --only off-userretry,naive-userretry,full-userretry
bash bench/scripts/with_lock.sh bash bench/scripts/microbench.sh
bash bench/scripts/wrk2_check.sh

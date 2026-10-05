#!/usr/bin/env bash
# Rest of the first round (after the fault-injection fix, BUG_LOG B3) and the
# second round.
cd "$(dirname "$0")/../.."
E="python3 bench/experiments.py"
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200
$E exp4 --cap 400 --conc 32 --slow-ms 400
$E exp3 --cap 400 --conc 32 --trigger hog
$E exp8 --cap 400 --conc 32
$E exp5 --cap 400 --conc 32
$E exp6 --cap 400 --conc 32
$E exp7 --cap 400 --conc 32
$E exp_conc_sweep --cap 350 --ns 48,64,128
$E exp1 --cap 400 --conc 32 --loads 3 --only off,ratelimit,fixedconc,gradient2,full --reps 2
$E exp1 --cap 400 --conc 32 --loads 1.5 --only fixedconc,gradient2
$E exp2 --cap 400 --conc 32 --reps 2
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200 --only off-userretry,naive-userretry,full-userretry
$E exp4 --cap 400 --conc 32 --slow-ms 400
bash bench/scripts/with_lock.sh bash bench/scripts/microbench.sh
bash bench/scripts/wrk2_check.sh
$E exp3 --cap 400 --conc 32 --trigger hogw
$E exp8 --cap 400 --conc 32 --trigger hogw
$E exp1 --cap 400 --conc 32 --loads 3 --only full-aimd --reps 2
$E exp5 --cap 400 --conc 32 --loads 0.5,0.9 --only full-aimd

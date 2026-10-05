#!/usr/bin/env bash
# M5: the headline experiments on a one-node k3d cluster instead of Compose.
# Runs the wrk2 cross-check first (it needs the Compose stack), then stops
# the Compose stack (same VM, same 2 vCPUs), brings up k3d, runs the 3x
# goodput comparison and the metastable-failure trigger, and tears down.
cd "$(dirname "$0")/../.."
export PATH=$HOME/lcwork/bin:$PATH
[ "$SKIP_WRK2" = 1 ] || bash bench/scripts/wrk2_check.sh || echo "wrk2 check failed"
bash bench/scripts/with_lock.sh bash -c "cd bench/hotel && docker compose stop && cd ../.. && bash bench/k8s/run_k3d.sh up" || { echo "k3d up failed"; exit 1; }
export LC_PLATFORM=k8s KUBECTL=$HOME/lcwork/bin/kubectl
E="python3 bench/experiments.py"
$E exp1 --cap 400 --conc 32 --loads 3 --only off,full --reps 2 --out results/k8s_exp1_goodput.jsonl
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200 --only off-userretry,full-userretry --reps 2 --out results/k8s_exp3_metastable.jsonl
bash bench/scripts/with_lock.sh bash -c "bash bench/k8s/run_k3d.sh down; cd bench/hotel && docker compose start"

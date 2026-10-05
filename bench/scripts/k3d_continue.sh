#!/usr/bin/env bash
# Continues the k3d phase when a bulk image import fails (it did once on
# this VM): imports the images one at a time, waits for the deployments,
# then runs the experiments and tears down.
cd "$(dirname "$0")/../.."
export PATH=$HOME/lcwork/bin:$PATH
pkill -f k3d_phase.sh
for i in loadcontrol/hotel:latest loadcontrol/tc:latest busybox:latest; do
  for try in 1 2 3; do
    k3d image import -c lc "$i" >/dev/null 2>&1
    docker exec k3d-lc-server-0 crictl images | grep -q "${i%%:*}" && break
  done
done
docker exec k3d-lc-server-0 crictl images | grep -E 'loadcontrol|busybox'
kubectl delete pod --all >/dev/null
kubectl wait --for=condition=available deploy --all --timeout=600s || exit 1
kubectl get pods
export LC_PLATFORM=k8s KUBECTL=$HOME/lcwork/bin/kubectl
E="python3 bench/experiments.py"
$E exp1 --cap 400 --conc 32 --loads 3 --only off,full --reps 2 --out results/k8s_exp1_goodput.jsonl
$E exp3 --cap 400 --conc 32 --trigger memc --slow-ms 200 --only off-userretry,full-userretry --reps 2 --out results/k8s_exp3_metastable.jsonl
bash bench/scripts/with_lock.sh bash -c "bash bench/k8s/run_k3d.sh down; cd bench/hotel && docker compose start"

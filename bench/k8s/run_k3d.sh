#!/usr/bin/env bash
# Runs the benchmark on a one-node k3d (k3s in Docker) cluster instead of
# Compose. Tools are downloaded into $WORK/bin (user space).
#   run_k3d.sh up      create the cluster, import images, deploy
#   run_k3d.sh down    delete the cluster
# Experiments then run with LC_PLATFORM=k8s KUBECTL=$WORK/bin/kubectl.
set -euo pipefail
WORK=${WORK:-$HOME/lcwork}
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=$WORK/bin
mkdir -p "$BIN"
export PATH=$BIN:$PATH

tools() {
  [ -x "$BIN/k3d" ] || { curl -sSLo "$BIN/k3d" https://github.com/k3d-io/k3d/releases/download/v5.7.4/k3d-linux-amd64; chmod +x "$BIN/k3d"; }
  [ -x "$BIN/kubectl" ] || { curl -sSLo "$BIN/kubectl" https://dl.k8s.io/release/v1.31.2/bin/linux/amd64/kubectl; chmod +x "$BIN/kubectl"; }
}

up() {
  tools
  python3 "$HERE/gen.py"
  k3d cluster create lc --servers 1 --agents 0 \
    -p "5000:5000@loadbalancer" -p "9090:9090@loadbalancer" \
    --k3s-arg "--disable=traefik@server:0" --k3s-arg "--disable=metrics-server@server:0" --wait
  k3d image import -c lc loadcontrol/hotel:latest hashicorp/consul:1.20 jaegertracing/all-in-one:1.62.0 \
    memcached:1.6 mongo:5.0 prom/prometheus:v3.0.1
  kubectl apply -f "$HERE/hotel.yaml"
  kubectl wait --for=condition=available deploy --all --timeout=300s
}

down() { tools; k3d cluster delete lc; }

"$@"

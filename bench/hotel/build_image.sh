#!/usr/bin/env bash
# Patches hotelReservation, vendors dependencies in a Go container and
# builds the image loadcontrol/hotel:latest.
set -euo pipefail
LC=${LC:-/mnt/c/Mac/Documents/LoadControl}
WORK=${WORK:-$HOME/lcwork}
GO=$WORK/go/bin/go bash "$LC/bench/hotel/apply_patch.sh" "$WORK/DeathStarBench" "$LC" "$WORK/hotel"
mkdir -p "$WORK/gomodcache"
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e GOFLAGS=-mod=mod \
  -e GOCACHE=/tmp/gocache -e GOPATH=/tmp/gopath -e GOMODCACHE=/gomod \
  -v "$WORK/gomodcache:/gomod" -v "$WORK/hotel:/src" -w /src golang:1.24 \
  sh -c 'go mod tidy && go mod vendor && go build -mod=vendor ./...'
cd "$WORK/hotel"
git add -A >/dev/null
git diff --cached -- . ':!vendor' ':!third_party' ':!go.sum' > "$WORK/hotel.patch"
docker build -q -t loadcontrol/hotel:latest .

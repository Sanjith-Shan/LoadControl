#!/usr/bin/env bash
# Builds a patched copy of DeathStarBench hotelReservation with LoadControl
# wired in. Usage: apply_patch.sh <DeathStarBench checkout> <LoadControl repo> <out dir>
# The patch is small on purpose: one new package (lcwire), the tracing
# interceptor in each server becomes a chain that ends in LoadControl, the
# dialer chains the LoadControl client interceptor, and the frontend's mux
# is wrapped in the HTTP middleware. Application code is not touched.
set -euo pipefail
DSB=${1:?DeathStarBench checkout}
LC=${2:?LoadControl repo}
OUT=${3:?output dir}
PIN=6ecb09706140f8730b5385c08f1386c654c3c526
got=$(git -C "$DSB" rev-parse HEAD)
[ "$got" = "$PIN" ] || echo "warning: DeathStarBench at $got, patch written against $PIN" >&2

rm -rf "$OUT" && mkdir -p "$OUT"
cp -r "$DSB/hotelReservation/." "$OUT/"
( cd "$OUT" && git init -q && git add -A && git -c user.email=x -c user.name=x commit -qm upstream )
mkdir -p "$OUT/third_party/loadcontrol"
( cd "$LC" && tar --exclude=./build --exclude=./results --exclude=./bench --exclude=./.git --exclude=./sim --exclude=./cmd -cf - . ) | tar -xf - -C "$OUT/third_party/loadcontrol"
cp -r "$LC/bench/hotel/_overlay/lcwire" "$OUT/lcwire"
cd "$OUT"

# 1. gRPC servers: tracing interceptor -> tracing + LoadControl chain.
for f in services/*/server.go; do
  perl -0pi -e 's/grpc\.UnaryInterceptor\(\s*otgrpc\.OpenTracingServerInterceptor\(s\.Tracer\),\s*\),/lcwire.ServerOption(otgrpc.OpenTracingServerInterceptor(s.Tracer)),/s' "$f"
  if grep -q 'lcwire.ServerOption' "$f"; then
    perl -0pi -e 's/import \(\n/import (\n\t"github.com\/delimitrou\/DeathStarBench\/tree\/master\/hotelReservation\/lcwire"\n/' "$f"
  fi
done
# 2. Dialer: chain the LoadControl client interceptor after tracing.
perl -0pi -e 's/return grpc\.WithUnaryInterceptor\(otgrpc\.OpenTracingClientInterceptor\(tracer\)\), nil/return grpc.WithChainUnaryInterceptor(otgrpc.OpenTracingClientInterceptor(tracer), lcwire.ClientInterceptor(name)), nil/' dialer/dialer.go
perl -0pi -e 's/import \(\n/import (\n\t"github.com\/delimitrou\/DeathStarBench\/tree\/master\/hotelReservation\/lcwire"\n/' dialer/dialer.go
# 3. Frontend: wrap the mux in the HTTP middleware.
perl -0pi -e 's/Handler: mux,/Handler: lcwire.HTTP(mux),/' services/frontend/server.go
perl -0pi -e 's/import \(\n/import (\n\t"github.com\/delimitrou\/DeathStarBench\/tree\/master\/hotelReservation\/lcwire"\n/' services/frontend/server.go
# 4. Module: depend on the library through a local replace.
sed -i 's/^FROM golang:1.21 as builder/FROM golang:1.24 AS builder/' Dockerfile
perl -0pi -e 's/COPY tune\/ tune\//COPY tune\/ tune\/\nCOPY lcwire\/ lcwire\/\nCOPY third_party\/ third_party\//' Dockerfile
${GO:-go} mod edit -require=github.com/Sanjith-Shan/LoadControl@v0.0.0 -replace=github.com/Sanjith-Shan/LoadControl=./third_party/loadcontrol
echo "patched: $(git diff --stat -- . ':!vendor' ':!third_party' | tail -1)"

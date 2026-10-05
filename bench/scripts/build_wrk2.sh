#!/usr/bin/env bash
# Builds DeathStarBench's wrk2 into the image loadcontrol/wrk2.
set -e
DSB=${DSB:-$HOME/lcwork/DeathStarBench}
git -C "$DSB" submodule update --init --depth 1 wrk2/deps/luajit
docker build -q -t loadcontrol/wrk2 -f "$(dirname "$0")/../wrk2/Dockerfile" "$DSB"

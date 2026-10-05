#!/usr/bin/env bash
# Per-container CPU at a steady rate: cpu_profile.sh <rate> <seconds>
cd "$(dirname "$0")/../.."
./build/loadgen-linux -rate $1 -duration ${2}s -out /tmp/cpuprof.jsonl &
sleep $(( $2 / 2 ))
docker stats --no-stream --format '{{.CPUPerc}} {{.Name}}' | sort -rn | head -14
top -bn1 | head -15 | tail -9
wait
tail -1 /tmp/cpuprof.jsonl | cut -c1-300

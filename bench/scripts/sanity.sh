#!/usr/bin/env bash
cd "$(dirname "$0")/../.."
python3 bench/experiments.py exp1 --cap ${CAP:-350} --only ${ONLY:-gradient2,full} --reps 1 2>&1

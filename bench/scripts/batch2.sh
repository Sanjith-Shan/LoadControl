#!/usr/bin/env bash
# Main experiments, one repetition each, in order of importance.
cd "$(dirname "$0")/../.."
E="python3 bench/experiments.py"
$E exp1 --cap 400 --conc 32
$E exp3 --cap 400 --conc 32
$E exp2 --cap 400 --conc 32
$E exp4 --cap 400 --conc 32 --slow-ms 400
$E exp5 --cap 400 --conc 32
$E exp6 --cap 400 --conc 32
$E exp7 --cap 400 --conc 32

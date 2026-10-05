#!/usr/bin/env python3
"""Prints a run's per-second series compactly: series.py <file> [name-substring] [step]"""
import json
import sys

path = sys.argv[1]
want = sys.argv[2] if len(sys.argv) > 2 else ""
step = int(sys.argv[3]) if len(sys.argv) > 3 else 5
for l in open(path):
    r = json.loads(l)
    if want not in r["name"]:
        continue
    print(f"== {r['name']} faults={[(f['t'], f['fault']) for f in r['faults']]}")
    ser = r["series"]
    for i in range(0, len(ser), step):
        w = ser[i:i + step]
        g = sum(s["good"] for s in w) / len(w)
        att = sum(s["attempts"] for s in w) / len(w)
        to = sum(s["timeout"] for s in w) / len(w)
        sh = sum(s["shed"] for s in w) / len(w)
        er = sum(s["error"] for s in w) / len(w)
        print(f"t={ser[i]['t']:3}-{ser[i]['t'] + len(w) - 1:3} good={g:6.1f} attempts={att:6.1f} timeout={to:6.1f} shed={sh:6.1f} err={er:6.1f}")

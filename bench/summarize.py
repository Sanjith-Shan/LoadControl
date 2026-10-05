#!/usr/bin/env python3
"""Prints one line per run from results/*.jsonl files.

  python bench/summarize.py results/exp1_goodput.jsonl [...]
"""
import json
import sys


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else float("nan")


def line(r):
    s, L = r["summary"], r["load"]
    d = r["duration_s"]
    shed = s.get("shed", 0)
    t0 = s.get("tiers_summary", {}).get("0", {})
    rs = sum(v or 0 for v in r.get("restarts", {}).values())
    return (f"{r['exp']:6} {r['name']:28} off={s['offered']/d:6.0f}/s good={s['good']/d:6.1f}/s "
            f"slow={s['slow']:5} shed={shed:5} err={s['error']:5} to={s['timeout']:5} "
            f"p50={s['p50_ms']} p99={s['p99_ms']} t0p99={t0.get('p99_ms')} t0ok={t0.get('success_rate', float('nan')):.3f} "
            f"cpu={mean(L['wsl_cpu_pct_per_s']):.0f}% win={L['before'].get('windows_cpu_pct')} "
            f"restarts={rs} peer={L['before'].get('peer_lock') or L['after'].get('peer_lock') or '-'}")


for path in sys.argv[1:]:
    for l in open(path):
        print(line(json.loads(l)))

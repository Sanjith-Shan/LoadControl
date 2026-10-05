#!/usr/bin/env python3
"""Computes every reported figure from results/*.jsonl and prints markdown
tables (the source for NUMBERS.md). Nothing here is typed in by hand: each
figure names the file and run names it came from.

  python bench/lcnumbers.py [--cap N]
"""
import argparse
import glob
import json
import math
import os
import statistics as st
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(REPO, "results")


def load(name):
    p = os.path.join(RES, name)
    if not os.path.exists(p):
        return []
    return [json.loads(l) for l in open(p)]


def smean(xs):
    xs = [x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))]
    return st.mean(xs) if xs else float("nan")


def good_rate(r):
    return r["summary"]["good"] / r["duration_s"]


def cpu(r):
    xs = r["load"]["wsl_cpu_pct_per_s"]
    return sum(xs) / len(xs) if xs else float("nan")


def host_clean(r):
    L = r["load"]
    around = any(((L.get(k) or {}).get("windows_cpu_pct") or 0) >= 90 for k in ("before", "after"))
    mid = ((L.get("mid") or {}).get("windows_cpu_pct") or 0) >= 72
    hs = L.get("host_cpu_pct_per_2s") or []
    cont = bool(hs) and sorted(hs)[len(hs) // 2] >= 72
    peer = any((L.get(k) or {}).get("peer_lock") for k in ("before", "mid", "after"))
    return not (around or mid or cont or peer)


def clean(r):
    """A run is clean if no peer lock was seen, no service restarted, and
    nothing outside the benchmark competed for the host. The WSL VM
    saturating its 2 vCPUs shows as 55-65% host CPU; a mid-run sample (or the
    median of the continuous samples, where recorded) at 72% or more means
    another process took a core, and 90% before or after means the host was
    busy around the run."""
    L = r["load"]
    peer = any((L.get(k) or {}).get("peer_lock") for k in ("before", "mid", "after"))
    rs = sum(v or 0 for v in r.get("restarts", {}).values())
    around = any(((L.get(k) or {}).get("windows_cpu_pct") or 0) >= 90 for k in ("before", "after"))
    mid = ((L.get("mid") or {}).get("windows_cpu_pct") or 0) >= 72
    hs = L.get("host_cpu_pct_per_2s") or []
    cont = bool(hs) and sorted(hs)[len(hs) // 2] >= 72
    return not (peer or rs or around or mid or cont)


def fmt(x, d=1):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    return f"{x:,.{d}f}"


def by_name(rows):
    g = defaultdict(list)
    for r in rows:
        g[r["name"]].append(r)
    return g


def capacity():
    rows = [r for r in load("exp0_capacity.jsonl") if clean(r)]
    g = by_name(rows)
    pts = []
    for name, rs in g.items():
        pts.append((rs[0]["rate"], st.mean(good_rate(r) for r in rs), len(rs), min(good_rate(r) for r in rs),
                    max(good_rate(r) for r in rs), st.mean(cpu(r) for r in rs)))
    pts.sort()
    print("## exp0 capacity (no control), results/exp0_capacity.jsonl, clean runs only\n")
    print("| offered req/s | goodput req/s (mean) | runs | min | max | VM CPU % |")
    print("|---|---|---|---|---|---|")
    for p in pts:
        print(f"| {p[0]:.0f} | {fmt(p[1])} | {p[2]} | {fmt(p[3])} | {fmt(p[4])} | {fmt(p[5], 0)} |")
    cap = max(pts, key=lambda p: p[1]) if pts else None
    if cap:
        print(f"\nCapacity (peak mean goodput): **{cap[1]:.0f} req/s** at {cap[0]:.0f} offered.\n")
    return cap[1] if cap else None


def table(file, title, cap, extra=None):
    rows = load(file)
    if not rows:
        return
    print(f"## {title}, results/{file}\n")
    print("| run | n | goodput req/s | % of capacity | shed | errors | timeouts | p50 ms | p99 ms | tier0 p99 ms | tier0 success | VM CPU % | clean/all |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name, rs_all in by_name(rows).items():
        rs = [r for r in rs_all if clean(r)] or rs_all
        gr = [good_rate(r) for r in rs]
        s = [r["summary"] for r in rs]
        t0 = [x.get("tiers_summary", {}).get("0", {}) for x in s]
        print(f"| {name} | {len(rs)} | {fmt(smean(gr))} | {fmt(100 * smean(gr) / cap) if cap else 'n/a'} | "
              f"{sum(x['shed'] for x in s) // len(s)} | {sum(x['error'] for x in s) // len(s)} | {sum(x['timeout'] for x in s) // len(s)} | "
              f"{fmt(smean(x['p50_ms'] for x in s if x['p50_ms'] is not None))} | "
              f"{fmt(smean(x['p99_ms'] for x in s if x['p99_ms'] is not None))} | "
              f"{fmt(smean(t['p99_ms'] for t in t0 if t.get('p99_ms') is not None))} | "
              f"{fmt(smean(t.get('success_rate', float('nan')) for t in t0), 3)} | "
              f"{fmt(smean(cpu(r) for r in rs), 0)} | {sum(clean(r) for r in rs_all)}/{len(rs_all)} |")
    print()


def recovery(r, base_from=20, on=30, off=50, frac=0.9, hold=10):
    """Seconds after the trigger is removed until goodput (per completion
    second) stays at >= frac of the pre-trigger mean for `hold` seconds.
    None if it never does before the run ends."""
    ser = r["series"]
    good = {s["t"]: s["good"] for s in ser}
    base = st.mean(good.get(t, 0) for t in range(base_from, on))
    end = r["duration_s"]
    for t in range(off, end - hold + 1):
        if all(good.get(u, 0) >= frac * base for u in range(t, t + hold)):
            return t - off, base
    return None, base


def exp3():
    rows = load("exp3_metastable.jsonl")
    if not rows:
        return
    print("## exp3 metastable recovery, results/exp3_metastable.jsonl\n")
    print("Trigger at 30 s (caches flushed, MongoDB +latency), removed at 50 s. Recovery = seconds after removal until goodput stays >= 90% of its pre-trigger mean for 10 s.\n")
    print("| run | pre-trigger goodput | goodput 100-150 s | recovery s | user attempts per request | clean |")
    print("|---|---|---|---|---|---|")
    for r in rows:
        rec, base = recovery(r)
        late = [s["good"] for s in r["series"] if 100 <= s["t"] < 150]
        att = r["summary"]["attempts"] / max(1, r["summary"]["offered"])
        print(f"| {r['name']} | {fmt(base)} | {fmt(st.mean(late) if late else float('nan'))} | "
              f"{'never (run ends 100 s after)' if rec is None else rec} | {fmt(att, 2)} | {clean(r)} |")
    print()


def exp4():
    rows = load("exp4_amplification.jsonl")
    if not rows:
        return
    print("## exp4 retry amplification during a dependency fault, results/exp4_amplification.jsonl\n")
    print("The rate service's cache gets +400 ms (netem) from 10 s to 50 s at 0.3x capacity, so every rate call "
          "misses its 250 ms per-try timeout. Users retry twice (3 s timeout). Counts are per user search request "
          "over the whole run, from client-side counters (a service that crashes resets its own counters, so the "
          "callers' counts are used). Restarts here are an outcome of the storm, not contamination.\n")
    print("| run | n | user attempts per request | frontend -> search attempts per search | search -> rate attempts per search | rate restarts | host clean |")
    print("|---|---|---|---|---|---|---|")
    g = defaultdict(list)
    for r in rows:
        g[r["name"]].append(r)
    for name, rs in g.items():
        vals = []
        for r in rs:
            ca = r["prometheus"].get("client_attempts", {})
            fs = sum(v for k, v in ca.items() if "service=frontend" in k and "target=srv-search" in k)
            sr = sum(v for k, v in ca.items() if "service=srv-search" in k and "target=srv-rate" in k)
            n = 0.6 * r["summary"]["offered"]
            ks = r["summary"].get("kinds_offered", {})
            if ks.get("search"):
                n = ks["search"]
            vals.append((r["summary"]["attempts"] / r["summary"]["offered"], fs / n, sr / n,
                         (r.get("restarts") or {}).get("rate") or 0, host_clean(r)))
        m = lambda i: smean(v[i] for v in vals)
        print(f"| {name} | {len(rs)} | {fmt(m(0), 2)} | {fmt(m(1), 2)} | {fmt(m(2), 2)} | "
              f"{sum(v[3] for v in vals)} | {sum(v[4] for v in vals)}/{len(vals)} |")
    print()


def exp3_table(file, title, trigger_desc):
    rows = load(file)
    if not rows:
        return
    print(f"## {title}, results/{file}\n")
    print(f"{trigger_desc} Recovery = seconds after the trigger is removed until goodput stays at >= 90% of its "
          "pre-trigger mean (20-30 s) for 10 s; 'never' means not before the run ended.\n")
    print("| run | pre-trigger goodput | trigger cleared at s | goodput during last 50 s | recovery s | user attempts per request | clean |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        off = next((f["t"] for f in r["faults"] if f["fault"] == "clear"), None)
        rec, base = recovery(r, off=int(round(off)) if off else 50) if r["faults"] else (None, None)
        late = [s["good"] for s in r["series"] if r["duration_s"] - 50 <= s["t"] < r["duration_s"]]
        att = r["summary"]["attempts"] / max(1, r["summary"]["offered"])
        recs = "n/a (no trigger)" if not r["faults"] else ("never" if rec is None else str(rec))
        print(f"| {r['name']} | {fmt(base)} | {fmt(off)} | {fmt(smean(late))} | {recs} | {fmt(att, 2)} | {clean(r)} |")
    print()


def window_table(file, title, desc, windows):
    rows = load(file)
    if not rows:
        return
    print(f"## {title}, results/{file}\n")
    print(desc + "\n")
    print("| run | n | " + " | ".join(f"goodput {a}-{b} s" for a, b in windows) + " | shed | clean/all |")
    print("|---|---|" + "---|" * len(windows) + "---|---|")
    for name, rs_all in by_name(rows).items():
        rs = [r for r in rs_all if clean(r)] or rs_all
        cells = []
        for a, b in windows:
            cells.append(fmt(smean(smean(s["good"] for s in r["series"] if a <= s["t"] < b) for r in rs)))
        shed = sum(r["summary"]["shed"] for r in rs) // len(rs)
        print(f"| {name} | {len(rs)} | " + " | ".join(cells) + f" | {shed} | {sum(clean(r) for r in rs_all)}/{len(rs_all)} |")
    print()


def sim_table():
    rows = load("sim_replay.jsonl")
    if not rows:
        return
    print("## M3 simulator against every recorded run, results/sim_replay.jsonl\n")
    print("One calibrated parameter set (sim/params.calibrated.json) replays each run's config, load, faults and "
          "user behavior. Error is simulated minus measured goodput. Relative error only where measured goodput "
          "is at least 5 req/s. Contaminated runs (clean() rule) are excluded.\n")
    print("| experiment | runs | median abs error req/s | median rel error | worst rel error | within 10% |")
    print("|---|---|---|---|---|---|")
    g = defaultdict(list)
    for r in rows:
        if r.get("contaminated") or r.get("skipped"):
            continue
        g[r["exp"]].append(r)
    allr = []
    for e in sorted(g):
        rs = g[e]
        ab = [abs(r["abs_err_good_rps"]) for r in rs if r.get("abs_err_good_rps") is not None]
        rel = [abs(r["rel_err_good"]) for r in rs if r.get("rel_err_good") is not None and r["measured"]["good_rps"] >= 5]
        allr += rs
        print(f"| {e} | {len(rs)} | {fmt(st.median(ab)) if ab else 'n/a'} | {fmt(100 * st.median(rel)) + '%' if rel else 'n/a'} | "
              f"{fmt(100 * max(rel)) + '%' if rel else 'n/a'} | {sum(x <= 0.1 for x in rel)}/{len(rel)} |")
    ab = [abs(r["abs_err_good_rps"]) for r in allr if r.get("abs_err_good_rps") is not None]
    rel = [abs(r["rel_err_good"]) for r in allr if r.get("rel_err_good") is not None and r["measured"]["good_rps"] >= 5]
    if ab:
        print(f"| all | {len(allr)} | {fmt(st.median(ab))} | {fmt(100 * st.median(rel))}% | {fmt(100 * max(rel))}% | {sum(x <= 0.1 for x in rel)}/{len(rel)} |")
    print()


def microbench():
    rows = load("exp7_microbench.jsonl")
    if not rows:
        return
    r = rows[-1]
    b = r["benchmarks"]
    print("## exp7 middleware overhead (Go benchmarks), results/exp7_microbench.jsonl\n")
    print(f"Run on the benchmark VM ({r['machine']['cpu']}, {r['machine']['vcpus']} vCPUs) while holding the bench "
          f"lock, nothing else running; median of 6 runs of 2 s each.\n")
    print("| benchmark | ns/op | B/op | allocs/op |")
    print("|---|---|---|---|")
    for k in sorted(b):
        print(f"| {k} | {fmt(b[k]['ns_op_median'], 0)} | {b[k]['bytes_op']} | {b[k]['allocs_op']} |")
    for path in ("bufconn", "tcp"):
        bare = b.get(f"BenchmarkUnaryBare/{path}")
        full = b.get(f"BenchmarkUnaryInterceptors/{path}")
        if bare and full:
            print(f"\nAdded per call over {path}: **{(full['ns_op_median'] - bare['ns_op_median']) / 1000:.1f} us** "
                  f"({bare['ns_op_median'] / 1000:.1f} us bare, {full['ns_op_median'] / 1000:.1f} us with every piece on).")
    print()


def wrk2():
    rows = load("wrk2_crosscheck.jsonl")
    if not rows:
        return
    print("## loadgen cross-checked against the benchmark's wrk2, results/wrk2_crosscheck.jsonl\n")
    print("Same no-control stack, same rates, the benchmark's own wrk2 and Lua script against cmd/loadgen.\n")
    print("| rate | tool | achieved req/s | p50 ms | p99 ms |")
    print("|---|---|---|---|---|")
    for r in rows:
        if r["name"].startswith("wrk2"):
            print(f"| {r['rate']} | wrk2 | {fmt(r['requests_per_s'])} | {fmt(r['p50_ms'])} | {fmt(r['p99_ms'])} |")
        else:
            s = r["summary"]
            print(f"| {r['rate']} | loadgen | {fmt((s['good'] + s['slow']) / r['duration_s'])} | {fmt(s['p50_ms'])} | {fmt(s['p99_ms'])} |")
    print()


def step_table():
    rows = [r for r in load("exp6_algorithms.jsonl") if r["name"].endswith("-step")]
    if not rows:
        return
    print("## exp6 step response, results/exp6_algorithms.jsonl (`-step` runs)\n")
    print("Offered 0.5x (200 req/s) for 20 s, 3x (1,200 req/s) from 20 s to 60 s, then 0.5x again. Goodput "
          "averaged per window; 'back to 90%' is the first second after 60 s from which goodput stays at >= 180 "
          "req/s for 5 s.\n")
    print("| run | 5-20 s | 25-60 s | 65-90 s | back to 90% after s | p99 ms (whole run) | clean |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        g = {x["t"]: x["good"] for x in r["series"]}
        w = lambda a, b: smean(g.get(t, 0) for t in range(a, b))
        back = next((t - 60 for t in range(60, 86) if all(g.get(u, 0) >= 180 for u in range(t, t + 5))), None)
        print(f"| {r['name']} | {fmt(w(5, 20))} | {fmt(w(25, 60))} | {fmt(w(65, 90))} | "
              f"{'never' if back is None else back} | {fmt(r['summary']['p99_ms'])} | {clean(r)} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=float, default=0)
    a = ap.parse_args()
    cap = capacity()
    if a.cap:
        cap = a.cap
    table("exp1_fixedconc_sweep.jsonl", "static concurrency limit sweep at 2x", cap)
    table("exp1_goodput.jsonl", "exp1 goodput under overload", cap)
    table("exp2_priority.jsonl", "exp2 priority at 3x capacity (tiers 20/30/50%)", cap)
    exp3_table("exp3_coldcache_only.jsonl", "exp3, first round: cold caches only",
               "Caches flushed at 30 s; the MongoDB latency this round also asked for never reached the services (BUG_LOG B3).")
    exp3_table("exp3_metastable.jsonl", "exp3 metastable recovery",
               "0.7x capacity (280 req/s, ramped over 15 s), users retry 3 times on a 1 s timeout. Trigger from 30 s: caches flushed and 200 ms of netem latency on the three caches (`-memc`), or a CPU-hogging container (`-hog`).")
    exp4()
    window_table("exp8_capacity_shift.jsonl", "exp8 capacity shift",
                 "Offered load 0.9x of the measured capacity (360 req/s). A busy-loop container takes one of the "
                 "two vCPUs from 30 s to 90 s. Goodput per second averaged over each window.",
                 [(10, 30), (40, 90), (95, 120)])
    table("exp5_false_shedding.jsonl", "exp5 shedding below capacity", cap)
    table("exp6_algorithms.jsonl", "exp6 algorithm comparison", cap)
    step_table()
    table("exp7_overhead_e2e.jsonl", "exp7 end-to-end overhead at low load", cap)
    microbench()
    wrk2()
    table("k8s_exp1_goodput.jsonl", "M5 on Kubernetes (one-node k3d on the same VM): goodput at 1,200 req/s offered", cap)
    exp3_table("k8s_exp3_metastable.jsonl", "M5 on Kubernetes: metastable recovery",
               "Same trigger and load as exp3 `-memc`, on one-node k3d (the k3s control plane and kube-proxy share the same 2 vCPUs, so capacity is lower than on Compose and was not remeasured; read the goodput as absolute req/s, not as a share of the Compose capacity).")
    sim_table()


if __name__ == "__main__":
    main()

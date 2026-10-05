#!/usr/bin/env python3
"""Experiment plans. Each experiment writes the env files it needs under
bench/hotel/configs/gen/ and runs bench/lcbench.py once per point, appending
to results/<exp>.jsonl.

  python3 bench/experiments.py exp1 --cap 300 [--only name,...] [--reps 1]

--cap is the measured no-control capacity (peak goodput, req/s) from exp0.
"""
import argparse
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(REPO, "bench", "hotel", "configs", "gen")

# Building blocks. Keys without a service prefix apply to every service.
DEADLINE = {"LC_DEADLINE": "on", "LC_FRONTEND_DEFAULT_TIMEOUT_MS": "1000"}
# Chosen in results/tuning.jsonl (variant C): the least shedding below
# capacity and the most goodput at 3x of the three settings tried.
SHARES = {"LC_TIER_SHARES": "1,0.9,0.8"}
QUEUE = {"LC_QUEUE_WAIT_MS": "20"}
BUDGET = {"LC_RETRY": "budget", "LC_RETRY_ATTEMPTS": "3", "LC_PER_TRY_TIMEOUT_MS": "250",
          "LC_FRONTEND_PER_TRY_TIMEOUT_MS": "800"}
NAIVE = {"LC_RETRY": "naive", "LC_RETRY_ATTEMPTS": "3", "LC_PER_TRY_TIMEOUT_MS": "250",
         "LC_FRONTEND_PER_TRY_TIMEOUT_MS": "800"}
ONE_LAYER = {"LC_ONE_LAYER": "on"}
THROTTLE = {"LC_FRONTEND_THROTTLE": "2", "LC_FRONTEND_THROTTLE_WINDOW_S": "30"}


def lim(alg):
    return {"LC_LIMIT": alg, "LC_LIMIT_MIN": "8"}


def merge(*ds):
    out = {}
    for d in ds:
        out.update(d)
    return out


def full(alg="gradient2"):
    """Every piece on: the configuration the README's headline numbers use."""
    return merge(lim(alg), DEADLINE, SHARES, QUEUE, BUDGET, ONE_LAYER, THROTTLE)


def write_env(name, env, comment):
    os.makedirs(GEN, exist_ok=True)
    path = os.path.join(GEN, name + ".env")
    with open(path, "w") as f:
        f.write(f"# {comment}\n")
        for k, v in env.items():
            f.write(f"{k}={v}\n")
    return path


def run(exp, name, env, comment, out, rate=0, schedule="", duration=60, faults="", loadgen="", note="", warmup=15):
    path = write_env(f"{exp}_{name}", env, comment)
    cmd = [sys.executable, os.path.join(REPO, "bench", "lcbench.py"), "--exp", exp, "--name", name,
           "--env", path, "--duration", str(duration), "--out", out, "--warmup", str(warmup)]
    if schedule:
        cmd += ["--schedule", schedule]
    else:
        cmd += ["--rate", str(rate)]
    if faults:
        cmd += ["--faults", faults]
    if loadgen:
        cmd += ["--loadgen", loadgen]
    if note:
        cmd += ["--note", note]
    print(">>", exp, name, rate or schedule, flush=True)
    r = subprocess.run(cmd, text=True, capture_output=True)
    print((r.stdout.strip().splitlines() or [""])[-1], r.stderr.strip()[-400:] if r.returncode else "", flush=True)


def baselines(cap, conc):
    return {
        "off": ({}, "no control"),
        "ratelimit": ({"LC_FRONTEND_RATELIMIT": str(cap)}, f"static token bucket at the frontend at measured capacity {cap}/s"),
        "fixedconc": ({"LC_FRONTEND_LIMIT": f"fixed:{conc}"}, f"static concurrency limit {conc} at the frontend"),
    }


def exp1(a):
    out = a.out or os.path.join(REPO, "results", "exp1_goodput.jsonl")
    cfgs = baselines(a.cap, a.conc)
    cfgs["gradient2"] = (merge(lim("gradient2"), DEADLINE), "Gradient2 at every service + deadline drop")
    cfgs["full"] = (full(), "LoadControl, every piece on")
    for rep in range(a.reps):
        for x in (a.loads or [1, 1.5, 2, 3, 4]):
            for name, (env, c) in cfgs.items():
                if a.only and name not in a.only:
                    continue
                run("exp1", f"{name}-{x}x", env, c, out, rate=round(a.cap * x), note=f"rep {rep}")


def exp_conc_sweep(a):
    """Pick the static concurrency baseline fairly: the best fixed limit at 2x."""
    out = a.out or os.path.join(REPO, "results", "exp1_fixedconc_sweep.jsonl")
    for n in (a.ns or [2, 4, 8, 16, 32]):
        run("exp1sweep", f"fixedconc{n}-2x", {"LC_FRONTEND_LIMIT": f"fixed:{n}"},
            f"static concurrency limit {n} at the frontend", out, rate=round(a.cap * 2))


def exp2(a):
    out = a.out or os.path.join(REPO, "results", "exp2_priority.jsonl")
    cfgs = baselines(a.cap, a.conc)
    cfgs.update({
        "gradient2-notiers": (merge(lim("gradient2"), DEADLINE), "Gradient2 + deadline, tiers ignored"),
        "gradient2-shares": (merge(lim("gradient2"), DEADLINE, SHARES), "Gradient2 + deadline + tier shares"),
        "gradient2-shares-queue": (merge(lim("gradient2"), DEADLINE, SHARES, QUEUE), "Gradient2 + deadline + tier shares + 5 ms priority queue"),
        "dagor-wait": (merge(lim("gradient2"), DEADLINE, QUEUE, {"LC_DAGOR": "wait", "LC_DAGOR_THRESHOLD_MS": "5"}), "Gradient2 + deadline + DAGOR level on limiter queue wait (5 ms)"),
        "full+dagor": (merge(full(), {"LC_DAGOR": "wait", "LC_DAGOR_THRESHOLD_MS": "5"}), "LoadControl, every piece on, plus the DAGOR level"),
        "dagor-sched": (merge(DEADLINE, {"LC_DAGOR": "sched", "LC_DAGOR_THRESHOLD_MS": "2"}), "DAGOR level on Go scheduler latency, no limiter"),
        "full": (full(), "LoadControl, every piece on"),
    })
    for rep in range(a.reps):
        for name, (env, c) in cfgs.items():
            if a.only and name not in a.only:
                continue
            run("exp2", name, env, c, out, rate=round(a.cap * 3), note=f"rep {rep}; tiers 0.2/0.3/0.5")


TRIGGER_ON, TRIGGER_OFF = 30, 50
TRIGGER = (f"{TRIGGER_ON}s=flush:rate,{TRIGGER_ON}s=flush:profile,{TRIGGER_ON}s=flush:reserve,"
           f"{TRIGGER_ON}s=latency:mongo-rate:{{ms}},{TRIGGER_ON}s=latency:mongo-profile:{{ms}},"
           f"{TRIGGER_ON}s=latency:mongo-reservation:{{ms}},{TRIGGER_OFF}s=clear")
USER_RETRIES = "-retries 3 -timeout 1s"


TRIGGER_MEMC = (f"{TRIGGER_ON}s=flush:rate,{TRIGGER_ON}s=flush:profile,{TRIGGER_ON}s=flush:reserve,"
                f"{TRIGGER_ON}s=latency:memc-rate:{{ms}},{TRIGGER_ON}s=latency:memc-profile:{{ms}},"
                f"{TRIGGER_ON}s=latency:memc-reserve:{{ms}},{TRIGGER_OFF}s=clear")
TRIGGER_HOG = f"{TRIGGER_ON}s=hog:1,{TRIGGER_OFF}s=clear"
TRIGGER_HOGW = f"{TRIGGER_ON}s=hogw:1,{TRIGGER_OFF}s=clear"


def exp3(a):
    out = a.out or os.path.join(REPO, "results", "exp3_metastable.jsonl")
    x = a.x or 0.7
    # mongo: cold caches + slow database (too weak to tip this system, kept
    # as data); memc: cold caches + slow cache tier; hog: a noisy neighbour
    # takes one of the two vCPUs.
    faults = {"mongo": TRIGGER, "memc": TRIGGER_MEMC, "hog": TRIGGER_HOG, "hogw": TRIGGER_HOGW}[a.trigger].format(ms=a.slow_ms)
    cfgs = {
        "off-userretry": ({}, "no control; users retry 3 times on timeout or error"),
        "naive-userretry": (NAIVE, "naive retries at every hop (3 attempts, per-try timeouts); users retry 3 times"),
        "budget-userretry": (BUDGET, "retry budget at every hop, no limiter; users retry 3 times"),
        "full-userretry": (full(), "LoadControl, every piece on; users retry 3 times"),
        "off-notrigger": ({}, "no control, users retry, no trigger (control run)"),
    }
    for rep in range(a.reps):
        for name, (env, c) in cfgs.items():
            if a.only and name not in a.only:
                continue
            f = "" if name.endswith("notrigger") else faults
            r = round(a.cap * x)
            # Ramp up over 15 s: starting cold at full rate with user retries
            # can tip the system into the bad state before the trigger.
            ramp = f"0s:{r // 4},5s:{r // 2},10s:{3 * r // 4},15s:{r}"
            tag = "" if a.trigger == "mongo" else f"-{a.trigger}"
            run("exp3", f"{name}-{x}x{tag}", env, c, out, schedule=ramp, duration=150, faults=f,
                loadgen=USER_RETRIES, note=f"rep {rep}; trigger {a.trigger} {TRIGGER_ON}-{TRIGGER_OFF}s ({a.slow_ms} ms)")


def exp4(a):
    """Retry amplification during a dependency fault: memcached for rate gets
    slow enough that every rate call misses its 250 ms per-try timeout."""
    out = a.out or os.path.join(REPO, "results", "exp4_amplification.jsonl")
    faults = f"10s=latency:memc-rate:{a.slow_ms},50s=clear"
    lg = "-retries 2 -timeout 3s"
    cfgs = {
        "naive": (NAIVE, "naive retries at every hop: 3 attempts, per-try 250 ms (frontend 800 ms)"),
        "budget": (BUDGET, "gRFC A6 retry budget at every hop"),
        "onelayer": (merge(NAIVE, ONE_LAYER), "3 attempts at every hop but only the layer next to the failure retries"),
        "budget-onelayer": (merge(BUDGET, ONE_LAYER), "retry budget + one layer"),
    }
    for rep in range(a.reps):
        for name, (env, c) in cfgs.items():
            if a.only and name not in a.only:
                continue
            for honor in ([False, True] if "onelayer" in name else [False]):
                n = name + ("-userhonors" if honor else "")
                run("exp4", n, env, c + ("; users honor X-Lc-No-Retry" if honor else ""), out,
                    rate=round(a.cap * 0.3), duration=60, faults=faults,
                    loadgen=lg + (" -honor-no-retry" if honor else ""),
                    note=f"rep {rep}; memc-rate +{a.slow_ms} ms from 10 s to 50 s; users retry 2 times, 3 s timeout")


def exp5(a):
    out = a.out or os.path.join(REPO, "results", "exp5_false_shedding.jsonl")
    cfgs = {
        "full": (full(), "LoadControl, every piece on"),
        "aimd": (merge(lim("aimd"), DEADLINE), "AIMD + deadline"),
        "vegas": (merge(lim("vegas"), DEADLINE), "Vegas + deadline"),
        "gradient2": (merge(lim("gradient2"), DEADLINE), "Gradient2 + deadline"),
    }
    for rep in range(a.reps):
        for x in [0.25, 0.5, 0.75, 0.9]:
            for name, (env, c) in cfgs.items():
                if a.only and name not in a.only:
                    continue
                run("exp5", f"{name}-{x}x", env, c, out, rate=round(a.cap * x), note=f"rep {rep}")


def exp6(a):
    out = a.out or os.path.join(REPO, "results", "exp6_algorithms.jsonl")
    thr_alone = {"LC_FRONTEND_THROTTLE": "2", "LC_FRONTEND_THROTTLE_WINDOW_S": "30",
                 "LC_FRONTEND_THROTTLE_FAILURES": "on", "LC_FRONTEND_PER_TRY_TIMEOUT_MS": "500",
                 "LC_FRONTEND_RETRY": "none"}
    cfgs = {}
    for alg in ["aimd", "vegas", "gradient2"]:
        cfgs[alg] = (merge(lim(alg), DEADLINE), f"{alg} at every service + deadline")
        cfgs[alg + "+throttle"] = (merge(lim(alg), DEADLINE, THROTTLE), f"{alg} + deadline + SRE client throttle at the frontend")
    cfgs["throttle"] = (thr_alone, "SRE client throttle alone at the frontend, counting 500 ms timeouts as rejections")
    sched = f"0s:{round(a.cap * 0.5)},20s:{round(a.cap * 3)},60s:{round(a.cap * 0.5)}"
    for rep in range(a.reps):
        for name, (env, c) in cfgs.items():
            if a.only and name not in a.only:
                continue
            run("exp6", f"{name}-3x", env, c, out, rate=round(a.cap * 3), note=f"rep {rep}")
            run("exp6", f"{name}-step", env, c, out, schedule=sched, duration=90, note=f"rep {rep}; 0.5x, 3x at 20-60 s, 0.5x")


def tuning(a):
    """Parameter selection for the full configuration, kept as data: shed
    rate below capacity versus goodput at 3x for a few settings."""
    out = a.out or os.path.join(REPO, "results", "tuning.jsonl")
    variants = {
        "A-shares.6-q5": full(),
        "B-shares.8-q20": merge(full(), {"LC_TIER_SHARES": "1,0.9,0.8", "LC_QUEUE_WAIT_MS": "20"}),
        "C-shares.8-q20-min8": merge(full(), {"LC_TIER_SHARES": "1,0.9,0.8", "LC_QUEUE_WAIT_MS": "20", "LC_LIMIT_MIN": "8"}),
    }
    for name, env in variants.items():
        if a.only and name not in a.only:
            continue
        for x in (a.loads or [0.75, 3]):
            run("tuning", f"{name}-{x}x", env, "tuning the full configuration", out, rate=round(a.cap * x))


def exp8(a):
    """Capacity shift: a noisy neighbour takes one of the VM's two vCPUs from
    30 s to 90 s while the offered load stays at 0.9x of the measured
    capacity. A static limit tuned to the old capacity is now far above the
    new one; adaptive limits have to find the new one."""
    out = a.out or os.path.join(REPO, "results", "exp8_capacity_shift.jsonl")
    cfgs = baselines(a.cap, a.conc)
    cfgs["gradient2"] = (merge(lim("gradient2"), DEADLINE), "Gradient2 at every service + deadline drop")
    cfgs["full"] = (full(), "LoadControl, every piece on")
    for rep in range(a.reps):
        for name, (env, c) in cfgs.items():
            if a.only and name not in a.only:
                continue
            h = "hogw" if a.trigger == "hogw" else "hog"
            run("exp8", f"{name}-0.9x-{h}", env, c, out, rate=round(a.cap * 0.9), duration=120,
                faults=f"30s={h}:1,90s=clear", note=f"rep {rep}; {h} busy-loop container from 30 s to 90 s")


def exp7(a):
    """End-to-end overhead at low load: everything on but nothing to shed."""
    out = a.out or os.path.join(REPO, "results", "exp7_overhead_e2e.jsonl")
    for rep in range(a.reps):
        for name, env, c in [("off", {}, "no control"), ("full", full(), "LoadControl, every piece on")]:
            run("exp7", f"{name}-0.25x", env, c, out, rate=round(a.cap * 0.25), duration=120, note=f"rep {rep}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp")
    ap.add_argument("--cap", type=int, required=True)
    ap.add_argument("--conc", type=int, default=8, help="static concurrency baseline (best of exp_conc_sweep)")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--only", default="")
    ap.add_argument("--x", type=float, default=0)
    ap.add_argument("--slow-ms", type=int, default=100)
    ap.add_argument("--trigger", default="mongo", choices=["mongo", "memc", "hog", "hogw"])
    ap.add_argument("--loads", default="", help="override load multiples, e.g. 3,4")
    ap.add_argument("--ns", default="", help="static limits for exp_conc_sweep")
    ap.add_argument("--out", default="", help="override the results file")
    a = ap.parse_args()
    a.only = [s for s in a.only.split(",") if s]
    a.loads = [float(x) if "." in x else int(x) for x in a.loads.split(",") if x]
    a.ns = [int(x) for x in a.ns.split(",") if x]
    globals()[a.exp.replace("-", "_")](a)


if __name__ == "__main__":
    main()

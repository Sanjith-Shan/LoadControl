#!/usr/bin/env python3
"""Runs one LoadControl experiment on the hotelReservation stack and appends
one JSON line to a results file.

Each run: take the shared bench lock, restart the hotel services with the
given LC_* config, wait until healthy, warm the caches, record machine and
load, run the open-loop load generator with optional faults on a timeline,
then read per-service counters from Prometheus. The line holds the config,
the loadgen summary, the per-second series, the fault timeline, the
Prometheus counters, and the machine and load during the run.

Usage (inside WSL, from the repo):
  python3 bench/lcbench.py --exp exp1 --name off-1x --env bench/hotel/configs/off.env \
      --rate 300 --duration 60 --out results/exp1_goodput.jsonl
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOTEL = os.path.join(REPO, "bench", "hotel")
LOADGEN = os.path.join(REPO, "build", "loadgen-linux")
SERVICES = ["frontend", "search", "geo", "rate", "profile", "recommendation", "user", "reservation"]
LOCK = "/tmp/BENCH_LOCK"
ME = "loadcontrol"
PROM = "http://localhost:9090"
PLATFORM = os.environ.get("LC_PLATFORM", "compose")  # compose | k8s
KUBECTL = os.environ.get("KUBECTL", "kubectl")
TOXI = "http://localhost:8474"


def sh(cmd, check=True, capture=True, env=None):
    r = subprocess.run(cmd, shell=True, text=True, capture_output=capture, env=env)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}: {r.stderr.strip()}")
    return r.stdout.strip() if capture else ""


def http(method, url, body=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


YIELD_FILE = "/tmp/LC_LAST_YIELD"
BATCH_S = int(os.environ.get("LC_BATCH_S", "2400"))  # run at most this long before yielding
YIELD_S = int(os.environ.get("LC_YIELD_S", "200"))  # free window left for the peer session


def lock():
    if os.environ.get("LC_LOCK_HELD"):
        return  # a wrapper (scripts/with_lock.sh) already holds it
    # Take turns with the peer session sharing the machine: after BATCH_S of
    # back-to-back runs, leave the lock free for YIELD_S so it can start a job.
    try:
        last = float(open(YIELD_FILE).read())
    except (FileNotFoundError, ValueError):
        last = time.time()
        open(YIELD_FILE, "w").write(str(last))
    if time.time() - last > BATCH_S:
        print(f"yielding the bench lock for {YIELD_S} s", file=sys.stderr)
        time.sleep(YIELD_S)
        open(YIELD_FILE, "w").write(str(time.time()))
    while True:
        try:
            fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{ME} {os.getpid()} {time.strftime('%FT%T')}\n".encode())
            os.close(fd)
            return
        except FileExistsError:
            owner = open(LOCK).read().strip()
            if owner.startswith(ME):
                pid = owner.split()[1]
                if not os.path.exists(f"/proc/{pid}"):
                    os.remove(LOCK)  # stale lock from a crashed run of ours
                    continue
            print(f"waiting for bench lock held by: {owner}", file=sys.stderr)
            time.sleep(15)
            open(YIELD_FILE, "w").write(str(time.time()))  # the peer had its turn


def unlock():
    if os.environ.get("LC_LOCK_HELD"):
        return
    try:
        if open(LOCK).read().startswith(ME):
            os.remove(LOCK)
    except FileNotFoundError:
        pass


def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + v[4]
    return sum(v), idle


def machine():
    model = ""
    with open("/proc/cpuinfo") as f:
        for line in f:
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    mem = int(open("/proc/meminfo").readline().split()[1]) // 1024
    return {"host": "mini PC (Acemagic K1)", "cpu": model, "vm": "WSL2 Ubuntu-24.04",
            "vcpus": os.cpu_count(), "mem_mb": mem, "docker": sh("docker version --format '{{.Server.Version}}'", check=False)}


def windows_cpu():
    """Host-wide CPU use as Windows sees it (the WSL VM shares 4 cores with Windows)."""
    try:
        out = sh("powershell.exe -NoProfile -Command \"(Get-Counter '\\Processor(_Total)\\% Processor Time' -SampleInterval 1 -MaxSamples 2).CounterSamples | Measure-Object CookedValue -Average | % Average\"", check=False)
        return round(float(out.strip().splitlines()[-1]), 1)
    except Exception:
        return None


def restarts():
    out = {}
    for s in SERVICES:
        if PLATFORM == "k8s":
            v = sh(f"{KUBECTL} get pods -l app={s} -o jsonpath='{{.items[0].status.containerStatuses[0].restartCount}}'", check=False)
        else:
            v = sh(f"docker inspect lchotel-{s}-1 --format '{{{{.RestartCount}}}}'", check=False)
        out[s] = int(v) if v.isdigit() else None
    return out


def kexec(deploy, cmd):
    return sh(f"{KUBECTL} exec deploy/{deploy} -- {cmd}", check=False)


def recreate(env_file):
    if PLATFORM == "k8s":
        sh(f"{KUBECTL} create configmap lc-env --from-env-file={env_file} --dry-run=client -o yaml | {KUBECTL} apply -f -")
        sh(f"{KUBECTL} rollout restart deploy " + " ".join(SERVICES))
        for s in SERVICES:
            sh(f"{KUBECTL} rollout status deploy/{s} --timeout=180s")
    else:
        compose("up -d --force-recreate --no-deps " + " ".join(SERVICES), env_file)


def reset_db():
    if PLATFORM == "k8s":
        js = ("db.adminCommand({listDatabases:1}).databases.filter(d=>![\"admin\",\"config\",\"local\"].includes(d.name))"
              ".forEach(d=>db.getSiblingDB(d.name).dropDatabase())")
        for m in ["geo", "profile", "rate", "recommendation", "reservation", "user"]:
            kexec(f"mongodb-{m}", f"mongo --quiet --eval '{js}'")
    else:
        sh(f"bash {REPO}/bench/scripts/reset_db.sh", check=False)


def load_snapshot():
    others = [n for n in sh("docker ps --format '{{.Names}}'", check=False).splitlines()
              if n and not n.startswith("lchotel-") and not n.startswith("k3d-lc")]
    procs = []
    for line in sh("ps -eo pcpu,comm --sort=-pcpu --no-headers", check=False).splitlines()[:12]:
        p, c = line.split(None, 1)
        if float(p) >= 5.0:
            procs.append(f"{c}:{p}")
    peer = None
    if os.path.exists(LOCK):
        o = open(LOCK).read().strip()
        if not o.startswith(ME):
            peer = o
    return {"loadavg": open("/proc/loadavg").read().split()[:3], "other_containers": others,
            "busy_procs": procs, "peer_lock": peer, "windows_cpu_pct": windows_cpu()}


class CPUMonitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self.stop = [], threading.Event()

    def run(self):
        t0, i0 = cpu_times()
        while not self.stop.wait(1.0):
            t1, i1 = cpu_times()
            if t1 > t0:
                self.samples.append(round(100.0 * (1 - (i1 - i0) / (t1 - t0)), 1))
            t0, i0 = t1, i1


def compose(args, env_file, check=True):
    env = dict(os.environ, LC_ENV=os.path.abspath(env_file))
    return sh(f"cd {HOTEL} && docker compose {args}", check=check, env=env)


def healthy(n=5, timeout=120):
    url = "http://localhost:5000/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"
    ok, deadline = 0, time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                ok = ok + 1 if r.status == 200 else 0
        except Exception:
            ok = 0
        if ok >= n:
            return True
        time.sleep(0.5)
    return False


def reset_faults():
    try:
        for p in json.loads(http("GET", f"{TOXI}/proxies")).values():
            for t in p.get("toxics", []):
                http("DELETE", f"{TOXI}/proxies/{p['name']}/toxics/{t['name']}")
            if not p.get("enabled", True):
                http("POST", f"{TOXI}/proxies/{p['name']}", {"enabled": True})
    except Exception as e:
        print("toxiproxy reset:", e, file=sys.stderr)


def flush(cache):
    c = "bash -c 'exec 3<>/dev/tcp/127.0.0.1/11211; printf \"flush_all\\r\\n\" >&3; head -c 4 <&3'"
    if PLATFORM == "k8s":
        kexec(f"memcached-{cache}", c)
    else:
        sh(f"docker exec lchotel-memcached-{cache}-1 {c}", check=False)


def apply_fault(spec):
    """Fault specs: flush:<rate|profile|reserve>, latency:<proxy>:<ms>[:<jitter>],
    down:<proxy>, up:<proxy>, clear, cpu:<container>:<cpus> (docker update)."""
    kind, *a = spec.split(":")
    if kind == "flush":
        flush(a[0])
    elif kind == "latency":
        jitter = int(a[2]) if len(a) > 2 else 0
        http("POST", f"{TOXI}/proxies/{a[0]}/toxics", {"name": f"lat_{a[0]}", "type": "latency", "stream": "downstream",
                                                       "attributes": {"latency": int(a[1]), "jitter": jitter}})
    elif kind == "down":
        http("POST", f"{TOXI}/proxies/{a[0]}", {"enabled": False})
    elif kind == "up":
        http("POST", f"{TOXI}/proxies/{a[0]}", {"enabled": True})
    elif kind == "clear":
        reset_faults()
        sh("docker rm -f $(docker ps -aq --filter name=lchotel-hog) 2>/dev/null || true", check=False)
    elif kind == "hog":
        # A noisy neighbour: one busy-looping container per CPU asked for,
        # sharing the VM's vCPUs with the services. hog:<n> starts, hog:0 stops.
        sh("docker rm -f $(docker ps -aq --filter name=lchotel-hog) 2>/dev/null || true", check=False)
        for i in range(int(a[0])):
            sh(f"docker run -d --rm --name lchotel-hog{i} busybox sh -c 'while :; do :; done'")
    elif kind == "cpu":
        sh(f"docker update --cpus {a[1]} lchotel-{a[0]}-1")
    else:
        raise ValueError(spec)


def prom_query(q, at):
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": f"{at:.3f}"})
    try:
        res = json.loads(http("GET", url))["data"]["result"]
    except Exception as e:
        return {"error": str(e)}
    out = {}
    for r in res:
        key = ",".join(f"{k}={v}" for k, v in sorted(r["metric"].items()) if k not in ("instance", "job"))
        out[key or "value"] = round(float(r["value"][1]), 3)
    return out


COUNTERS = {
    "inbound": "sum by (service,kind) (lc_inbound_total)",
    "server": "sum by (service,tier,outcome) (lc_requests_total)",
    "client_attempts": "sum by (service,target,kind) (lc_client_attempts_total)",
    "client_local": "sum by (service,target,reason) (lc_client_local_total)",
    "client_failures": "sum by (service,target) (lc_client_failures_total)",
}


def counters(at):
    """Raw counter values (differences of two snapshots are exact, unlike
    increase(), which extrapolates)."""
    out = {}
    for k, q in COUNTERS.items():
        r = prom_query(q, at)
        out[k] = {} if "error" in r else r
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--env", required=True, help="LC_* env file for the services")
    ap.add_argument("--rate", type=float, default=0)
    ap.add_argument("--schedule", default="")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=15)
    ap.add_argument("--warmup-rate", type=float, default=100)
    ap.add_argument("--faults", default="", help="comma list of <sec>s=<fault spec>")
    ap.add_argument("--loadgen", default="", help="extra loadgen flags")
    ap.add_argument("--out", required=True)
    ap.add_argument("--keep", action="store_true", help="do not restart services first")
    ap.add_argument("--note", default="")
    a = ap.parse_args()

    lock()
    try:
        reset_faults()
        if not a.keep:
            reset_db()
            recreate(a.env)
        if not healthy():
            raise RuntimeError("stack not healthy")
        # Prometheus keeps connections to old container IPs after a
        # recreate and would scrape the wrong service; restart it.
        if PLATFORM == "compose":
            sh("docker restart lchotel-prometheus-1")
        for _ in range(60):
            try:
                http("GET", f"{PROM}/-/ready")
                break
            except Exception:
                time.sleep(0.5)
        for c in ["rate", "profile", "reserve"]:
            flush(c)
        if a.warmup > 0:
            sh(f"{LOADGEN} -rate {a.warmup_rate} -duration {a.warmup}s -out /tmp/lc_warmup.jsonl")
        before = load_snapshot()
        mon = CPUMonitor()
        mon.start()
        lg = f"{LOADGEN} -duration {a.duration}s -label {shlex.quote(a.name)} -out /tmp/lc_run.jsonl {a.loadgen}"
        lg += f" -schedule {shlex.quote(a.schedule)}" if a.schedule else f" -rate {a.rate}"
        time.sleep(2)
        r0 = restarts()
        c0 = counters(time.time())
        t0 = time.time()
        proc = subprocess.Popen(lg, shell=True)
        timeline = []
        faults = []
        for f in filter(None, a.faults.split(",")):
            at, spec = f.split("=", 1)
            faults.append((float(at.rstrip("s")), spec))
        for at, spec in sorted(faults):
            delay = t0 + 0.05 + at - time.time()
            if delay > 0:
                time.sleep(delay)
            apply_fault(spec)
            timeline.append({"t": round(time.time() - t0, 2), "fault": spec})
        mid = load_snapshot() if a.duration >= 20 else None
        proc.wait()
        t1 = time.time()
        mon.stop.set()
        after = load_snapshot()
        reset_faults()
        sh("docker rm -f $(docker ps -aq --filter name=lchotel-hog) 2>/dev/null || true", check=False)
        time.sleep(2)  # one more scrape
        c1 = counters(time.time())
        r1 = restarts()
        prom = {k: {s: round(v - c0[k].get(s, 0), 1) for s, v in c1[k].items()} for k in c1 if k != "limit_avg"}
        prom["limit_avg"] = prom_query(f"avg_over_time(lc_limit[{int(t1 - t0) + 1}s])", t1)
        rows = [json.loads(l) for l in open("/tmp/lc_run.jsonl")]
        series = [r for r in rows if r["type"] == "second"]
        summary = [r for r in rows if r["type"] == "summary"][0]
        env = {}
        for line in open(a.env):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k] = v
        rec = {"exp": a.exp, "name": a.name, "time": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(t0)),
               "env_file": os.path.relpath(a.env, REPO), "env": env, "loadgen_cmd": lg.replace(REPO + "/", ""),
               "rate": a.rate, "schedule": a.schedule, "duration_s": a.duration, "faults": timeline,
               "summary": summary, "series": series, "prometheus": prom,
               "restarts": {k: (r1[k] - r0[k]) if None not in (r1[k], r0[k]) else None for k in r0},
               "machine": dict(machine(), platform=PLATFORM), "load": {"before": before, "mid": mid, "after": after,
                                             "wsl_cpu_pct_per_s": mon.samples}, "note": a.note}
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "a") as f:
            f.write(json.dumps(rec) + "\n")
        good = summary["good"] / a.duration
        print(f"{a.exp}/{a.name}: offered {summary['offered']} good {summary['good']} ({good:.0f}/s) "
              f"slow {summary['slow']} shed {summary['shed']} err {summary['error']} timeout {summary['timeout']} "
              f"p50 {summary['p50_ms']} p99 {summary['p99_ms']} cpu~{sum(mon.samples)/max(1,len(mon.samples)):.0f}%")
    finally:
        unlock()


if __name__ == "__main__":
    main()

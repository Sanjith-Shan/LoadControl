#!/usr/bin/env python3
"""Draws the README charts as static SVG from results/*.jsonl (no
dependencies). Every point is a measured run; nothing is typed in.

  python bench/charts.py      # writes docs/*.svg
"""
import json
import os
import statistics as st
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(REPO, "results")
DOCS = os.path.join(REPO, "docs")

# Reference categorical palette, light mode, fixed order.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
FONT = "font-family='-apple-system,Segoe UI,Helvetica,Arial,sans-serif'"


def load(name):
    p = os.path.join(RES, name)
    return [json.loads(l) for l in open(p)] if os.path.exists(p) else []


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


class Chart:
    def __init__(self, title, subtitle, w=720, h=400, xlabel="", ylabel=""):
        self.w, self.h = w, h
        self.l, self.r, self.t, self.b = 64, 150, 64, 52
        self.out = [f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}' {FONT}>",
                    f"<rect width='{w}' height='{h}' fill='{SURFACE}'/>",
                    f"<text x='{self.l}' y='26' font-size='16' font-weight='600' fill='{INK}'>{title}</text>",
                    f"<text x='{self.l}' y='45' font-size='12' fill='{INK2}'>{subtitle}</text>"]
        self.xlabel, self.ylabel = xlabel, ylabel

    def scale(self, x0, x1, y0, y1):
        self.x0, self.x1, self.y0, self.y1 = x0, x1, y0, y1

    def X(self, x):
        return self.l + (x - self.x0) / (self.x1 - self.x0) * (self.w - self.l - self.r)

    def Y(self, y):
        return self.h - self.b - (y - self.y0) / (self.y1 - self.y0) * (self.h - self.t - self.b)

    def axes(self, xticks, yticks, xfmt=str, yfmt=str):
        for y in yticks:
            self.out.append(f"<line x1='{self.l}' x2='{self.w - self.r}' y1='{self.Y(y):.1f}' y2='{self.Y(y):.1f}' stroke='{GRID}' stroke-width='1'/>")
            self.out.append(f"<text x='{self.l - 8}' y='{self.Y(y) + 4:.1f}' font-size='11' fill='{INK2}' text-anchor='end'>{yfmt(y)}</text>")
        for x in xticks:
            self.out.append(f"<text x='{self.X(x):.1f}' y='{self.h - self.b + 18}' font-size='11' fill='{INK2}' text-anchor='middle'>{xfmt(x)}</text>")
        self.out.append(f"<text x='{(self.l + self.w - self.r) / 2:.0f}' y='{self.h - 10}' font-size='12' fill='{INK2}' text-anchor='middle'>{self.xlabel}</text>")
        self.out.append(f"<text transform='translate(16,{(self.t + self.h - self.b) / 2:.0f}) rotate(-90)' font-size='12' fill='{INK2}' text-anchor='middle'>{self.ylabel}</text>")

    def line(self, pts, color, label, dashed=False, markers=True):
        d = " ".join(f"{'M' if i == 0 else 'L'}{self.X(x):.1f},{self.Y(y):.1f}" for i, (x, y) in enumerate(pts))
        dash = " stroke-dasharray='5 4'" if dashed else ""
        self.out.append(f"<path d='{d}' fill='none' stroke='{color}' stroke-width='2' stroke-linejoin='round'{dash}><title>{label}</title></path>")
        if markers:
            for x, y in pts:
                self.out.append(f"<circle cx='{self.X(x):.1f}' cy='{self.Y(y):.1f}' r='4' fill='{color}' stroke='{SURFACE}' stroke-width='2'><title>{label}: {y:.0f}</title></circle>")

    def label_end(self, y, color, text):
        # direct label at the right edge, with a short colored key mark
        x = self.w - self.r + 10
        self.out.append(f"<line x1='{x}' x2='{x + 12}' y1='{y:.1f}' y2='{y:.1f}' stroke='{color}' stroke-width='3'/>")
        self.out.append(f"<text x='{x + 17}' y='{y + 4:.1f}' font-size='12' fill='{INK}'>{text}</text>")

    def band(self, x0, x1, text):
        self.out.append(f"<rect x='{self.X(x0):.1f}' y='{self.t}' width='{self.X(x1) - self.X(x0):.1f}' height='{self.h - self.t - self.b}' fill='#efeee9'/>")
        self.out.append(f"<text x='{(self.X(x0) + self.X(x1)) / 2:.1f}' y='{self.t + 14}' font-size='11' fill='{INK2}' text-anchor='middle'>{text}</text>")

    def save(self, name):
        self.out.append("</svg>")
        os.makedirs(DOCS, exist_ok=True)
        open(os.path.join(DOCS, name), "w", newline="\n").write("\n".join(self.out) + "\n")


def spread_labels(items, min_gap=16):
    """items: [(y_px, color, text)] -> nudged so labels do not collide."""
    items = sorted(items)
    for i in range(1, len(items)):
        if items[i][0] - items[i - 1][0] < min_gap:
            items[i] = (items[i - 1][0] + min_gap,) + items[i][1:]
    return items


def goodput_chart():
    rows = [r for r in load("exp1_goodput.jsonl") if clean(r)]
    cap = 400
    g = defaultdict(lambda: defaultdict(list))
    for r in rows:
        cfg, mult = r["name"].rsplit("-", 1)
        g[cfg][float(mult.rstrip("x"))].append(r["summary"]["good"] / r["duration_s"])
    names = {"full": "LoadControl (all pieces)", "fixedconc": "static concurrency limit (best of sweep)",
             "gradient2": "Gradient2 limit only", "ratelimit": "static rate limit at capacity", "off": "no control"}
    order = ["full", "fixedconc", "gradient2", "ratelimit", "off"]
    c = Chart("Goodput under overload", "hotelReservation on 2 vCPUs, responses within 500 ms per second; mean of clean runs",
              w=780, xlabel="offered load (multiple of measured capacity, 400 req/s)", ylabel="goodput (req/s)")
    c.r = 250
    c.scale(0.9, 4.1, 0, 450)
    c.axes([1, 1.5, 2, 3, 4], [0, 100, 200, 300, 400], xfmt=lambda x: f"{x:g}x")
    c.out.append(f"<line x1='{c.l}' x2='{c.w - c.r}' y1='{c.Y(cap):.1f}' y2='{c.Y(cap):.1f}' stroke='{INK2}' stroke-width='1' stroke-dasharray='2 3'/>")
    c.out.append(f"<text x='{c.X(4.05):.1f}' y='{c.Y(cap) - 5:.1f}' font-size='11' fill='{INK2}' text-anchor='end'>capacity</text>")
    labels = []
    for i, k in enumerate(order):
        if k not in g:
            continue
        pts = sorted((x, st.mean(v)) for x, v in g[k].items())
        c.line(pts, SERIES[i], names[k])
        labels.append((c.Y(pts[-1][1]), SERIES[i], names[k]))
    for y, col, t in spread_labels(labels):
        c.label_end(y, col, t)
    c.save("goodput.svg")


def metastable_chart(fname, want, title, subtitle, out):
    rows = [r for r in load(fname) if clean(r)]
    c = Chart(title, subtitle, w=780, xlabel="seconds", ylabel="goodput (req/s, 5 s mean)")
    c.r = 200
    c.scale(0, 150, 0, 320)
    c.band(30, 50, "trigger")
    c.axes([0, 30, 50, 100, 150], [0, 100, 200, 300])
    labels = []
    for i, (name, label) in enumerate(want):
        rs = [r for r in rows if r["name"] == name]
        if not rs:
            continue
        # mean over every clean run of this configuration, not a chosen one
        sers = [{s["t"]: s["good"] for s in r["series"]} for r in rs]
        pts = []
        for t in range(0, 150, 5):
            pts.append((t + 2.5, st.mean(st.mean(ser.get(u, 0) for u in range(t, t + 5)) for ser in sers)))
        label = f"{label} ({len(rs)} runs)" if len(rs) > 1 else label
        c.line(pts, SERIES[i], label, markers=False)
        labels.append((c.Y(pts[-1][1]), SERIES[i], label))
    for y, col, t in spread_labels(labels):
        c.label_end(y, col, t)
    c.save(out)


def hbars(title, subtitle, items, unit, out, vmax, fmt_v=lambda v: f"{v:.0f}"):
    """Single-series horizontal bars (one color, so no legend), values
    labelled at the bar end, 4 px rounded data end."""
    rowh, top, left = 34, 70, 250
    w, h = 780, top + rowh * len(items) + 40
    o = [f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}' {FONT}>",
         f"<rect width='{w}' height='{h}' fill='{SURFACE}'/>",
         f"<text x='24' y='26' font-size='16' font-weight='600' fill='{INK}'>{title}</text>",
         f"<text x='24' y='45' font-size='12' fill='{INK2}'>{subtitle}</text>"]
    span = w - left - 90
    for i, (label, v, highlight) in enumerate(items):
        y = top + i * rowh
        bw = max(2, span * v / vmax)
        col = SERIES[0] if highlight else "#9fb8d9"
        o.append(f"<text x='{left - 10}' y='{y + 17}' font-size='12' fill='{INK}' text-anchor='end'>{label}</text>")
        o.append(f"<path d='M{left},{y + 5} h{bw - 4:.1f} a4,4 0 0 1 4,4 v12 a4,4 0 0 1 -4,4 h-{bw - 4:.1f} z' fill='{col}'><title>{label}: {fmt_v(v)}{unit}</title></path>")
        o.append(f"<text x='{left + bw + 6:.1f}' y='{y + 18}' font-size='12' fill='{INK}'>{fmt_v(v)}{unit}</text>")
    o.append(f"<line x1='{left}' x2='{left}' y1='{top}' y2='{top + rowh * len(items)}' stroke='{INK2}' stroke-width='1'/>")
    o.append("</svg>")
    os.makedirs(DOCS, exist_ok=True)
    open(os.path.join(DOCS, out), "w", newline="\n").write("\n".join(o) + "\n")


def priority_chart():
    rows = [r for r in load("exp2_priority.jsonl") if clean(r)]
    g = defaultdict(list)
    for r in rows:
        g[r["name"]].append(r["summary"]["tiers_summary"]["0"].get("success_rate", 0) * 100)
    names = [("off", "no control", False), ("ratelimit", "static rate limit", False),
             ("fixedconc", "static concurrency limit", False), ("gradient2-notiers", "Gradient2, no tiers", False),
             ("gradient2-shares", "Gradient2 + tier shares", True), ("gradient2-shares-queue", "+ 20 ms priority queue", True),
             ("full", "LoadControl, all pieces", True)]
    items = [(label, st.mean(g[k]), hl) for k, label, hl in names if g.get(k)]
    hbars("Top-priority requests served at 3x overload", "tier 0 is 20% of traffic; mean of clean runs (results/exp2_priority.jsonl)",
          items, "%", "priority.svg", 100)


def amplification_chart():
    rows = load("exp4_amplification.jsonl")
    g = defaultdict(list)
    for r in rows:
        ca = r["prometheus"].get("client_attempts", {})
        sr = sum(v for k, v in ca.items() if "service=srv-search" in k and "target=srv-rate" in k)
        g[r["name"]].append(sr / (0.6 * r["summary"]["offered"]))
    names = [("naive", "3 attempts at every hop", False), ("onelayer", "one layer, timeouts not nested", False),
             ("budget", "retry budget at every hop", True), ("budget-onelayer-userhonors", "budget + one layer, users honor it", True)]
    items = [(label, st.mean(g[k]), hl) for k, label, hl in names if g.get(k)]
    hbars("Requests reaching the failing service per user request", "rate's cache +400 ms for 40 s; users retry twice (results/exp4_amplification.jsonl)",
          items, "x", "amplification.svg", 16, fmt_v=lambda v: f"{v:.2f}")


if __name__ == "__main__":
    goodput_chart()
    metastable_chart("exp3_metastable.jsonl",
                     [("full-userretry-0.7x-memc", "LoadControl"), ("off-userretry-0.7x-memc", "no control"),
                      ("naive-userretry-0.7x-memc", "naive retries every hop")],
                     "Recovering from a retry storm",
                     "0.7x capacity, users retry 3 times; cold caches + 200 ms cache latency from 30 s to about 53 s; mean of clean runs",
                     "metastable.svg")
    priority_chart()
    amplification_chart()
    print("wrote", os.listdir(DOCS))

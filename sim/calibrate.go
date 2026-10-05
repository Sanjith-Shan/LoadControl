package sim

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// Measured is what calibration needs from real runs.
type Measured struct {
	// CapacityRPS is the highest goodput measured with no control (required).
	CapacityRPS float64
	// LatencyMS is the mean latency per request type (search, recommend,
	// user, reserve) measured at LowLoadRPS, where queueing is negligible.
	// Optional; types left out keep their prior handler costs.
	LatencyMS  map[string]float64
	LowLoadRPS float64
	// HiddenMS, if >= 0, fixes hidden_ms (CPU per user attempt outside the
	// service handlers, paid by shed attempts too) instead of fitting it;
	// it is identifiable only from runs that shed, such as a fixed
	// concurrency sweep. Negative means fit.
	HiddenMS float64
}

// handler names used by each request type, for the latency fit.
var typeHandlers = map[string][]string{
	"search":    {"frontend.search", "search.nearby", "geo.nearby", "rate.get", "reservation.check", "profile.get"},
	"recommend": {"frontend.recommend", "recommendation.get", "profile.get"},
	"user":      {"frontend.user", "user.check"},
	"reserve":   {"frontend.reserve", "user.check", "reservation.make"},
}

// Calibrate fits p's CPU costs to measurements in three steps.
//
//  1. Scale every CPU cost by one factor until the simulated peak goodput
//     with no control matches CapacityRPS. This fixes the total CPU per
//     user request.
//  2. If latencies are given, move CPU between the handlers on each
//     request type's path (damped iterative proportional fitting) until
//     the simulated mean latency per type at LowLoadRPS matches, keeping
//     the total fixed: what the paths do not need becomes hidden_ms, CPU
//     per user attempt off the critical path (kernel networking, GC,
//     tracing, the load generator sharing the VM). If the paths need more
//     than the total, the rest is fitted as non-CPU time per hop (net_ms).
//  3. Re-check the peak and trim hidden_ms.
//
// p's env and user settings should describe the measured no-control runs.
// Progress goes to log; the returned string is a report.
func Calibrate(p0 *Params, m Measured, log func(string)) (*Params, string, error) {
	if log == nil {
		log = func(string) {}
	}
	p := p0.Clone()
	p.Flush, p.Slow, p.Faults = Trigger{}, Trigger{}, nil
	p.CapacityRPS = m.CapacityRPS
	var rep []string
	addf := func(f string, a ...any) { rep = append(rep, fmt.Sprintf(f, a...)) }

	lat := map[string]float64{}
	for k, v := range m.LatencyMS {
		if k == "login" { // cmd/loadgen's name for the user request
			k = "user"
		}
		if typeHandlers[k] == nil {
			return nil, "", fmt.Errorf("unknown request type %q (want search, recommend, user, reserve)", k)
		}
		lat[k] = v
	}
	m.LatencyMS = lat
	var types []string
	for _, t := range reqNames {
		if m.LatencyMS[t] > 0 {
			types = append(types, t)
		}
	}
	if m.LowLoadRPS <= 0 {
		m.LowLoadRPS = 50
	}
	fixed := m.HiddenMS >= 0
	if fixed {
		p.HiddenMS = m.HiddenMS
	}

	fitPeak := func(step string, hiddenOnly bool) (float64, error) {
		var peak float64
		for it := range 8 {
			var err error
			if peak, err = peakGoodput(p, m.CapacityRPS); err != nil {
				return 0, err
			}
			log(fmt.Sprintf("%s %d: simulated peak %.1f req/s, measured %.1f, hidden_ms %.4f", step, it, peak, m.CapacityRPS, p.HiddenMS))
			f := peak / m.CapacityRPS
			if math.Abs(f-1) < 0.015 {
				break
			}
			if d := p.Cores * 1e3 * (1/m.CapacityRPS - 1/peak); hiddenOnly && !fixed && p.HiddenMS+d >= 0 {
				p.HiddenMS += d
			} else {
				scaleCPU(p, f, !fixed)
			}
		}
		return peak, nil
	}
	if _, err := fitPeak("capacity", false); err != nil {
		return nil, "", err
	}

	// With hidden_ms fixed the path total is pinned by capacity, so the
	// latency level goes to net_ms from the start.
	conflict := fixed
	var simLat map[string]float64
	if len(types) > 0 {
		mix := map[string]float64{}
		var tot float64
		for _, t := range reqNames {
			tot += p.Mix[t]
		}
		for _, t := range reqNames {
			mix[t] = p.Mix[t] / tot
		}
		// pathCPU is the mean handler CPU per user request.
		pathCPU := func() float64 {
			var w float64
			for _, t := range reqNames {
				for _, h := range typeHandlers[t] {
					w += mix[t] * p.Handlers[h]
				}
			}
			return w
		}
		for it := range 20 {
			var err error
			if simLat, err = lowLoad(p, m.LowLoadRPS); err != nil {
				return nil, "", err
			}
			worst, r := 0.0, map[string]float64{}
			for _, t := range types {
				r[t] = m.LatencyMS[t] / max(simLat[t], 1e-3)
				worst = max(worst, math.Abs(r[t]-1))
			}
			log(fmt.Sprintf("latency fit %d: sim %v, target %v, hidden_ms %.4f", it, fmtMap(simLat), fmtMap(m.LatencyMS), p.HiddenMS))
			if worst < 0.03 {
				break
			}
			if conflict {
				// Off the CPU now: one per-hop time (net_ms) takes the level
				// by least squares, the handlers only the shape.
				var num, den, lg float64
				for _, t := range types {
					h := float64(len(typeHandlers[t]))
					num += h * (m.LatencyMS[t] - simLat[t])
					den += h * h
					lg += math.Log(r[t])
				}
				p.NetMS = max(0, p.NetMS+0.8*num/den)
				g := math.Exp(lg / float64(len(types)))
				for t := range r {
					r[t] /= g
				}
			}
			before := pathCPU()
			// Each handler moves by the geometric mean of the ratios of the
			// types whose path it is on, damped and bounded per step since
			// latency near saturation is far from linear in CPU.
			for _, h := range sortedKeys(p.Handlers) {
				lg, n := 0.0, 0
				for _, t := range types {
					if slices.Contains(typeHandlers[t], h) {
						lg += math.Log(r[t])
						n++
					}
				}
				if n > 0 {
					p.Handlers[h] *= math.Max(0.67, math.Min(1.5, math.Exp(0.6*lg/float64(n))))
				}
			}
			if fixed {
				f := before / pathCPU()
				for h := range p.Handlers {
					p.Handlers[h] *= f
				}
				continue
			}
			p.HiddenMS += before - pathCPU()
			if p.HiddenMS < 0 {
				// The paths want more CPU than capacity allows: keep the
				// total, and fit the rest of the latency off the CPU.
				f := (pathCPU() + p.HiddenMS) / pathCPU()
				for h := range p.Handlers {
					p.Handlers[h] *= f
				}
				p.HiddenMS = 0
				if !conflict {
					addf("WARNING: the measured latencies imply more CPU on the request path than the measured capacity allows; the rest of the latency is fitted as non-CPU time per hop (net_ms)")
				}
				conflict = true
			}
		}
		if conflict {
			addf("net_ms fitted to %.4f", p.NetMS)
		}
	}
	peak, err := fitPeak("capacity check", true)
	if err != nil {
		return nil, "", err
	}
	if len(types) > 0 {
		if simLat, err = lowLoad(p, m.LowLoadRPS); err != nil {
			return nil, "", err
		}
		for _, t := range types {
			addf("latency %s at %.0f req/s: measured %.3f ms, simulated %.3f ms", t, m.LowLoadRPS, m.LatencyMS[t], simLat[t])
		}
	}
	addf("capacity: measured %.1f req/s, simulated peak %.1f req/s; hidden_ms %.4f, net_ms %.4f", m.CapacityRPS, peak, p.HiddenMS, p.NetMS)
	for _, h := range sortedKeys(p.Handlers) {
		addf("  %-20s %.4f ms (was %.4f)", h, p.Handlers[h], p0.Handlers[h])
	}
	addf("  rpc_server_ms %.4f rpc_client_ms %.4f memc_ms %.4f mongo.op_ms %.4f", p.RPCServerMS, p.RPCClientMS, p.MemcMS, p.Mongo.OpMS)
	p.Notes = []string{fmt.Sprintf("Calibrated by lcsim calibrate: capacity %.1f req/s, mean latencies %v at %.0f req/s.",
		m.CapacityRPS, fmtMap(m.LatencyMS), m.LowLoadRPS)}
	return p, strings.Join(rep, "\n"), nil
}

func scaleCPU(p *Params, f float64, hidden bool) {
	for h := range p.Handlers {
		p.Handlers[h] *= f
	}
	p.RPCServerMS *= f
	p.RPCClientMS *= f
	if hidden {
		p.HiddenMS *= f
	}
	p.MemcMS *= f
	p.MemcKeyMS *= f
	p.Mongo.OpMS *= f
	p.Mongo.KeyMS *= f
}

// lowLoad returns the simulated mean latency per type at rate rps with the
// configured mix, long enough for the rare types to get a few hundred
// samples.
func lowLoad(p *Params, rps float64) (map[string]float64, error) {
	q := p.Clone()
	q.Load = LoadParams{Process: p.Load.Process, RPS: rps}
	q.WarmupS = 5
	q.DurationS = min(400, max(30, 2e5/rps))
	r, err := Run(q)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, t := range reqNames {
		out[t] = r.Summary.Types[t].Mean
	}
	return out, nil
}

// peakGoodput finds the highest goodput over offered loads, bisecting for
// the knee: the largest load still served with goodput >= 95% of offered.
func peakGoodput(p *Params, guess float64) (float64, error) {
	best := 0.0
	try := func(rps float64) (bool, error) {
		q := p.Clone()
		q.Load = LoadParams{Process: p.Load.Process, RPS: rps}
		q.DurationS, q.WarmupS = 60, 10
		r, err := Run(q)
		if err != nil {
			return false, err
		}
		best = max(best, r.Summary.GoodputRPS)
		return r.Summary.GoodputRPS >= 0.95*r.Summary.OfferedRPS, nil
	}
	lo, hi := 0.25*guess, 4*guess
	for range 12 {
		mid := math.Sqrt(lo * hi)
		ok, err := try(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			lo = mid
		} else {
			hi = mid
		}
		if hi/lo < 1.01 {
			break
		}
	}
	return best, nil
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func fmtMap(m map[string]float64) string {
	var b strings.Builder
	for i, k := range sortedKeys(m) {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%.3f", k, m[k])
	}
	return b.String()
}

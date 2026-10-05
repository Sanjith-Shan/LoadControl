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
}

// handler names used by each request type, for the latency fit.
var typeHandlers = map[string][]string{
	"search":    {"frontend.search", "search.nearby", "geo.nearby", "rate.get", "reservation.check", "profile.get"},
	"recommend": {"frontend.recommend", "recommendation.get", "profile.get"},
	"user":      {"frontend.user", "user.check"},
	"reserve":   {"frontend.reserve", "user.check", "reservation.make"},
}

// Calibrate fits p's CPU costs to measurements. With latencies it first
// fits the handler costs on each request type's path to the low-load
// latencies by iterative proportional fitting, then sets hidden_ms (CPU
// per request off the critical path) so the simulated peak goodput with
// no control matches CapacityRPS. Without latencies it scales every CPU
// cost by one factor instead. p's env should describe the measured
// no-control run. Progress goes to log; the returned string is a report.
func Calibrate(p0 *Params, m Measured, log func(string)) (*Params, string, error) {
	if log == nil {
		log = func(string) {}
	}
	p := p0.Clone()
	p.Flush, p.Slow = Trigger{}, Trigger{}
	p.CapacityRPS = m.CapacityRPS
	var rep []string
	addf := func(f string, a ...any) { rep = append(rep, fmt.Sprintf(f, a...)) }

	// 1. Latency shape.
	var types []string
	for _, t := range reqNames {
		if m.LatencyMS[t] > 0 {
			types = append(types, t)
		}
	}
	for k := range m.LatencyMS {
		if typeHandlers[k] == nil {
			return nil, "", fmt.Errorf("unknown request type %q (want search, recommend, user, reserve)", k)
		}
	}
	if len(types) > 0 {
		if m.LowLoadRPS <= 0 {
			m.LowLoadRPS = 50
		}
		var lat map[string]float64
		for it := range 12 {
			var err error
			if lat, err = lowLoad(p, types, m.LowLoadRPS); err != nil {
				return nil, "", err
			}
			worst, r := 0.0, map[string]float64{}
			for _, t := range types {
				r[t] = m.LatencyMS[t] / lat[t]
				worst = max(worst, math.Abs(r[t]-1))
			}
			log(fmt.Sprintf("latency fit %d: sim %v, target %v", it, fmtMap(lat), fmtMap(m.LatencyMS)))
			if worst < 0.02 {
				break
			}
			// Each handler moves by the geometric mean of the ratios of the
			// types whose path it is on.
			for _, h := range sortedKeys(p.Handlers) {
				lg, n := 0.0, 0
				for _, t := range types {
					if slices.Contains(typeHandlers[t], h) {
						lg += math.Log(r[t])
						n++
					}
				}
				if n > 0 {
					p.Handlers[h] *= math.Exp(lg / float64(n))
				}
			}
		}
		for _, t := range types {
			addf("low-load latency %s: measured %.3f ms, simulated %.3f ms", t, m.LatencyMS[t], lat[t])
		}
	}

	// 2. Capacity.
	conflict := false
	for it := range 8 {
		peak, err := peakGoodput(p, m.CapacityRPS)
		if err != nil {
			return nil, "", err
		}
		f := peak / m.CapacityRPS
		log(fmt.Sprintf("capacity fit %d: simulated peak %.0f rps, measured %.0f rps", it, peak, m.CapacityRPS))
		if math.Abs(f-1) < 0.02 {
			break
		}
		if len(types) > 0 {
			// Change in CPU per request that moves the peak to the target.
			d := p.Cores * 1e3 * (1/m.CapacityRPS - 1/peak)
			if p.HiddenMS+d >= 0 {
				p.HiddenMS += d
				continue
			}
			addf("WARNING: the measured latencies imply more CPU per request than the measured capacity allows; scaling all CPU down and moving the rest of the latency to net_ms")
			p.HiddenMS = 0
			conflict = true
		}
		scaleCPU(p, f)
	}
	if conflict {
		// The latency the CPU no longer explains is time off the CPU:
		// least-squares fit of one per-hop network latency.
		for range 3 {
			lat, err := lowLoad(p, types, m.LowLoadRPS)
			if err != nil {
				return nil, "", err
			}
			var num, den float64
			for _, t := range types {
				h := float64(len(typeHandlers[t]))
				num += h * (m.LatencyMS[t] - lat[t])
				den += h * h
			}
			p.NetMS = max(0, p.NetMS+num/den)
		}
		lat, err := lowLoad(p, types, m.LowLoadRPS)
		if err != nil {
			return nil, "", err
		}
		addf("net_ms refit to %.4f to carry the non-CPU part of the latency", p.NetMS)
		for _, t := range types {
			addf("after capacity fit, low-load latency %s: measured %.3f ms, simulated %.3f ms", t, m.LatencyMS[t], lat[t])
		}
	}
	peak, err := peakGoodput(p, m.CapacityRPS)
	if err != nil {
		return nil, "", err
	}
	addf("capacity: measured %.0f rps, simulated peak %.0f rps; hidden_ms %.4f", m.CapacityRPS, peak, p.HiddenMS)
	for _, h := range sortedKeys(p.Handlers) {
		addf("  %-20s %.4f ms (was %.4f)", h, p.Handlers[h], p0.Handlers[h])
	}
	p.Notes = []string{fmt.Sprintf("Calibrated by lcsim calibrate: capacity %.0f rps, low-load latencies %v at %.0f rps.",
		m.CapacityRPS, fmtMap(m.LatencyMS), m.LowLoadRPS)}
	return p, strings.Join(rep, "\n"), nil
}

func scaleCPU(p *Params, f float64) {
	for h := range p.Handlers {
		p.Handlers[h] *= f
	}
	p.RPCServerMS *= f
	p.RPCClientMS *= f
	p.HiddenMS *= f
	p.MemcMS *= f
	p.MemcKeyMS *= f
	p.Mongo.OpMS *= f
	p.Mongo.KeyMS *= f
}

// lowLoad returns the simulated mean latency per type at rate rps, with an
// even mix over types so each gets enough samples.
func lowLoad(p *Params, types []string, rps float64) (map[string]float64, error) {
	q := p.Clone()
	q.Mix = map[string]float64{}
	for _, t := range types {
		q.Mix[t] = 1
	}
	q.Load = LoadParams{Process: "poisson", RPS: rps}
	q.WarmupS = 2
	q.DurationS = min(300, max(20, 3000*float64(len(types))/rps))
	r, err := Run(q)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, t := range types {
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
		q.Load = LoadParams{Process: "poisson", RPS: rps}
		q.DurationS, q.WarmupS = 40, 10
		r, err := Run(q)
		if err != nil {
			return false, err
		}
		best = max(best, r.Summary.GoodputRPS)
		return r.Summary.GoodputRPS >= 0.95*r.Summary.OfferedRPS, nil
	}
	lo, hi := 0.25*guess, 4*guess
	for range 10 {
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

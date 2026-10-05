package sim

import (
	"math"
	"slices"
	"time"
)

const nCache = 8 // caches tracked in stats

// sec accumulates one simulated second. Arrivals count in the second they
// arrive, outcomes in the second they end.
type sec struct {
	offered, completed, success, goodput int64
	out                                  [nOut]int64 // final outcomes as cmd/loadgen counts them
	tier                                 [3]struct{ offered, completed, success, goodput int64 }
	lat                                  [3][]float64 // ms, successful requests
	shed                                 [nSvc][nShed]int64
	att                                  [nSvc][2]int64 // inbound original, retry
	user                                 [2]int64
	leaf, mongo, conns                   int64
	looks, hits                          [nCache]int64
	local                                [nLocal]int64
	limit, inflight, queue               [nSvc]int
	util                                 float64
	jobs                                 int
}

// sec is the current second's stats; the warm-up phase goes to scratch.
func (s *Sim) sec() *sec {
	if s.now < s.t0 {
		return &s.scratch
	}
	i := int((s.now - s.t0) / 1e9)
	for len(s.secs) <= i {
		s.secs = append(s.secs, &sec{})
	}
	return s.secs[i]
}

// tickSecond snapshots gauges at the end of each second.
func (s *Sim) tickSecond() {
	i := int((s.now-s.t0)/1e9) - 1
	for len(s.secs) <= i {
		s.secs = append(s.secs, &sec{})
	}
	st := s.secs[i]
	for i, v := range s.svc {
		if v.lim != nil {
			st.limit[i], st.inflight[i], st.queue[i] = v.lim.alg.Limit(), v.lim.inflight, len(v.lim.queue)
		}
	}
	s.cpu.advance(s.now)
	if i == 0 {
		s.lastBusy = s.busyAtT0
	}
	st.util = (s.cpu.busy - s.lastBusy) / (s.cpu.cores * 1e9)
	s.lastBusy = s.cpu.busy
	st.jobs = s.cpu.load()
	if s.now < s.endNs {
		s.at(s.now+1e9, s.tickSecond)
	}
}

// endUser records a user request's single outcome.
func (s *Sim) endUser(u *ureq, o int) {
	s.outstanding--
	if u.warm {
		return
	}
	st := s.sec()
	t := min(int(u.in.Tier), 2)
	lat := float64(s.now-u.start) / 1e6
	st.completed++
	st.tier[t].completed++
	st.out[o]++
	window := s.now >= s.t0+ms(s.P.WarmupS*1e3) && s.now < s.endNs
	if window {
		s.typN[u.typ]++
	}
	if o == oGood || o == oSlow {
		st.success++
		st.tier[t].success++
		st.lat[t] = append(st.lat[t], lat)
		if o == oGood {
			st.goodput++
			st.tier[t].goodput++
		}
		if window {
			s.typOK[u.typ]++
			s.typLat[u.typ] = append(s.typLat[u.typ], lat)
		}
	}
}

// Result is a run's per-second series and summary.
type Result struct {
	Seconds []Second
	Summary Summary
}

type Second struct {
	Run         string                      `json:"run,omitempty"`
	T           int                         `json:"t"`
	Offered     int64                       `json:"offered"`
	Completed   int64                       `json:"completed"`
	Success     int64                       `json:"success"`
	Goodput     int64                       `json:"goodput"`
	Slow        int64                       `json:"slow"`
	UserShed    int64                       `json:"user_shed"` // final answer was a 503 shed
	Failed      int64                       `json:"failed"`    // other 5xx
	Timeout     int64                       `json:"timeout"`
	SuccessRate float64                     `json:"success_rate"`
	Tiers       map[string]TierSec          `json:"tiers"`
	Shed        map[string]map[string]int64 `json:"shed,omitempty"`
	ShedTotal   map[string]int64            `json:"shed_total,omitempty"`
	Attempts    map[string][2]int64         `json:"attempts"` // per service: [original, retry] inbound
	User        [2]int64                    `json:"user_attempts"`
	LeafPerReq  float64                     `json:"leaf_per_req"`
	MongoOps    int64                       `json:"mongo_ops"`
	NewConns    int64                       `json:"new_conns"` // connections the load generator opened (conn.proxy)
	CacheHit    map[string]float64          `json:"cache_hit"`
	Local       map[string]int64            `json:"client_local,omitempty"`
	Limit       map[string]int              `json:"limit,omitempty"`
	Inflight    map[string]int              `json:"inflight,omitempty"`
	Queue       map[string]int              `json:"queue,omitempty"`
	CPUUtil     float64                     `json:"cpu_util"`
	CPUJobs     int                         `json:"cpu_jobs"`
}

type TierSec struct {
	Offered     int64   `json:"offered"`
	Success     int64   `json:"success"`
	Goodput     int64   `json:"goodput"`
	SuccessRate float64 `json:"success_rate"`
	P50         float64 `json:"p50_ms"`
	P99         float64 `json:"p99_ms"`
}

type Summary struct {
	Run         string                      `json:"run,omitempty"`
	IsSummary   bool                        `json:"summary"`
	Env         string                      `json:"env"`
	Seed        uint64                      `json:"seed"`
	DurationS   float64                     `json:"duration_s"`
	WarmupS     float64                     `json:"warmup_s"`
	CapacityRPS float64                     `json:"capacity_rps"`
	OfferedRPS  float64                     `json:"offered_rps"`
	GoodputRPS  float64                     `json:"goodput_rps"`
	GoodputX    float64                     `json:"goodput_x"` // goodput / capacity_rps
	SuccessRate float64                     `json:"success_rate"`
	Tiers       map[string]TierSec          `json:"tiers"`
	Types       map[string]TypeSum          `json:"types"`
	Shed        map[string]map[string]int64 `json:"shed,omitempty"`
	Attempts    map[string][2]int64         `json:"attempts"`
	UserPerReq  float64                     `json:"user_attempts_per_req"`
	LeafPerReq  float64                     `json:"leaf_per_req"`
	MongoPerReq float64                     `json:"mongo_per_req"`
	CacheHit    map[string]float64          `json:"cache_hit"`
	MeanLimit   map[string]float64          `json:"mean_limit,omitempty"`
	CPUUtil     float64                     `json:"cpu_util"`
	RecoveryS   *float64                    `json:"recovery_s,omitempty"` // after the last trigger ends; null if it never recovers
	Totals      Totals                      `json:"totals"`               // whole run including the drain, as cmd/loadgen's summary
	Events      int64                       `json:"events"`
	WallMS      float64                     `json:"wall_ms"`
}

// Totals are user request outcomes over the whole run; they add up to Offered.
type Totals struct {
	Offered int64 `json:"offered"`
	Good    int64 `json:"good"`
	Slow    int64 `json:"slow"`
	Shed    int64 `json:"shed"`
	Error   int64 `json:"error"`
	Timeout int64 `json:"timeout"`
}

type TypeSum struct {
	N           int64   `json:"n"`
	SuccessRate float64 `json:"success_rate"`
	Mean        float64 `json:"mean_ms"`
	P50         float64 `json:"p50_ms"`
	P99         float64 `json:"p99_ms"`
}

func pct(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return xs[min(len(xs)-1, int(q*float64(len(xs))))]
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func round(x float64) float64 { return math.Round(x*1000) / 1000 }

func (s *Sim) result(wall time.Duration) *Result {
	r := &Result{}
	nsec := int(math.Ceil(s.P.DurationS))
	for len(s.secs) < nsec {
		s.secs = append(s.secs, &sec{})
	}
	sum := Summary{IsSummary: true, Env: s.P.EnvSummary(), Seed: s.P.Seed, DurationS: s.P.DurationS, WarmupS: s.P.WarmupS,
		CapacityRPS: s.P.CapacityRPS, Tiers: map[string]TierSec{}, Types: map[string]TypeSum{},
		Shed: map[string]map[string]int64{}, Attempts: map[string][2]int64{}, CacheHit: map[string]float64{},
		MeanLimit: map[string]float64{}, Events: s.events, WallMS: float64(wall.Microseconds()) / 1e3}
	var agg sec
	var tierLat [3][]float64
	var limSum [nSvc]float64
	n := 0
	for i, st := range s.secs {
		t := &sum.Totals
		t.Offered += st.offered
		t.Good += st.out[oGood]
		t.Slow += st.out[oSlow]
		t.Shed += st.out[oShed]
		t.Error += st.out[oError]
		t.Timeout += st.out[oTimeout]
		if i >= nsec {
			continue
		}
		r.Seconds = append(r.Seconds, s.second(i, st))
		if float64(i) < s.P.WarmupS {
			continue
		}
		n++
		agg.offered += st.offered
		agg.completed += st.completed
		agg.success += st.success
		agg.goodput += st.goodput
		agg.leaf += st.leaf
		agg.mongo += st.mongo
		agg.util += st.util
		agg.user[0] += st.user[0]
		agg.user[1] += st.user[1]
		for t := range 3 {
			a, b := &agg.tier[t], st.tier[t]
			a.offered += b.offered
			a.completed += b.completed
			a.success += b.success
			a.goodput += b.goodput
			tierLat[t] = append(tierLat[t], st.lat[t]...)
		}
		for c := range nCache {
			agg.looks[c] += st.looks[c]
			agg.hits[c] += st.hits[c]
		}
		for v := range nSvc {
			for k := range nShed {
				agg.shed[v][k] += st.shed[v][k]
			}
			agg.att[v][0] += st.att[v][0]
			agg.att[v][1] += st.att[v][1]
			limSum[v] += float64(st.limit[v])
		}
	}
	if n > 0 {
		f := float64(n)
		sum.OfferedRPS = round(float64(agg.offered) / f)
		sum.GoodputRPS = round(float64(agg.goodput) / f)
		if s.P.CapacityRPS > 0 {
			sum.GoodputX = round(sum.GoodputRPS / s.P.CapacityRPS)
		}
		sum.SuccessRate = round(ratio(agg.success, agg.completed))
		sum.CPUUtil = round(agg.util / f)
		for t, name := range tierNames {
			slices.Sort(tierLat[t])
			a := agg.tier[t]
			sum.Tiers[name] = TierSec{Offered: a.offered, Success: a.success, Goodput: a.goodput,
				SuccessRate: round(ratio(a.success, a.completed)), P50: round(pct(tierLat[t], .5)), P99: round(pct(tierLat[t], .99))}
		}
		for v, name := range svcNames {
			if s.svc[v].lim != nil {
				sum.MeanLimit[name] = round(limSum[v] / f)
			}
		}
	}
	for i, name := range reqNames {
		l := s.typLat[i]
		slices.Sort(l)
		var m float64
		for _, x := range l {
			m += x
		}
		if len(l) > 0 {
			m /= float64(len(l))
		}
		sum.Types[name] = TypeSum{N: s.typN[i], SuccessRate: round(ratio(s.typOK[i], s.typN[i])), Mean: round(m), P50: round(pct(l, .5)), P99: round(pct(l, .99))}
	}
	sum.Shed, _ = shedMaps(&agg)
	sum.Attempts = attMap(&agg)
	sum.UserPerReq = round(ratio(agg.user[0]+agg.user[1], agg.offered))
	sum.LeafPerReq = round(ratio(agg.leaf, agg.offered))
	sum.MongoPerReq = round(ratio(agg.mongo, agg.offered))
	sum.CacheHit = hitMap(s, &agg)
	sum.RecoveryS = s.recovery(r.Seconds)
	r.Summary = sum
	return r
}

func (s *Sim) second(i int, st *sec) Second {
	o := Second{T: i, Offered: st.offered, Completed: st.completed, Success: st.success, Goodput: st.goodput,
		Slow: st.out[oSlow], UserShed: st.out[oShed], Failed: st.out[oError], Timeout: st.out[oTimeout], SuccessRate: round(ratio(st.success, st.completed)),
		Tiers: map[string]TierSec{}, User: st.user, LeafPerReq: round(ratio(st.leaf, st.offered)), MongoOps: st.mongo, NewConns: st.conns,
		CPUUtil: round(st.util), CPUJobs: st.jobs, Limit: map[string]int{}, Inflight: map[string]int{}, Queue: map[string]int{}}
	for t, name := range tierNames {
		slices.Sort(st.lat[t])
		a := st.tier[t]
		o.Tiers[name] = TierSec{Offered: a.offered, Success: a.success, Goodput: a.goodput,
			SuccessRate: round(ratio(a.success, a.completed)), P50: round(pct(st.lat[t], .5)), P99: round(pct(st.lat[t], .99))}
	}
	o.Shed, o.ShedTotal = shedMaps(st)
	o.Attempts = attMap(st)
	o.CacheHit = hitMap(s, st)
	for v, name := range svcNames {
		if s.svc[v].lim != nil {
			o.Limit[name], o.Inflight[name], o.Queue[name] = st.limit[v], st.inflight[v], st.queue[v]
		}
	}
	o.Local = map[string]int64{}
	for k, name := range localNames {
		if st.local[k] > 0 {
			o.Local[name] = st.local[k]
		}
	}
	return o
}

func shedMaps(st *sec) (map[string]map[string]int64, map[string]int64) {
	by, tot := map[string]map[string]int64{}, map[string]int64{}
	for v, name := range svcNames {
		for k, reason := range shedNames {
			if x := st.shed[v][k]; x > 0 {
				if by[name] == nil {
					by[name] = map[string]int64{}
				}
				by[name][reason] = x
				tot[reason] += x
			}
		}
	}
	return by, tot
}

func attMap(st *sec) map[string][2]int64 {
	m := map[string][2]int64{}
	for v, name := range svcNames {
		m[name] = st.att[v]
	}
	return m
}

func hitMap(s *Sim, st *sec) map[string]float64 {
	m := map[string]float64{}
	for _, c := range s.caches {
		if st.looks[c.id] > 0 {
			m[c.name] = round(ratio(st.hits[c.id], st.looks[c.id]))
		}
	}
	return m
}

// recovery is how long after the last trigger ends goodput first gets back
// to 90% of its pre-trigger mean and stays there for 5 s. Nil if it never
// does within the run, or there is no trigger.
func (s *Sim) recovery(secs []Second) *float64 {
	var start, end float64
	for _, t := range []Trigger{s.P.Flush, s.P.Slow} {
		if t.At > 0 && (start == 0 || t.At < start) {
			start = t.At
		}
		end = max(end, t.At+t.Dur)
	}
	for _, f := range s.P.Faults {
		if start == 0 || f.At < start {
			start = f.At
		}
		end = max(end, f.At)
	}
	if start == 0 {
		return nil
	}
	var base float64
	n := 0
	for i := int(s.P.WarmupS); i < int(start) && i < len(secs); i++ {
		base += float64(secs[i].Goodput)
		n++
	}
	if n == 0 {
		return nil
	}
	base = 0.9 * base / float64(n)
	for i := int(math.Ceil(end)); i+5 <= len(secs); i++ {
		ok := true
		for j := i; j < i+5; j++ {
			ok = ok && float64(secs[j].Goodput) >= base
		}
		if ok {
			r := float64(i) - end
			return &r
		}
	}
	return nil
}

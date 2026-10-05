// Package limit holds adaptive concurrency limit algorithms and the
// limiter that enforces them.
//
// An Algorithm only computes a number: how many requests may be in flight.
// It learns from one sample per finished request (round-trip time, how many
// requests were in flight when it started, and whether it was dropped). The
// Limiter counts in-flight requests and refuses new ones once the count
// reaches the algorithm's current limit.
//
// AIMD, Vegas and Gradient2 are ports of the published algorithms as they
// appear in Netflix's concurrency-limits library (Java, Apache 2.0). The
// constants and update rules follow that code; DESIGN.md lists the
// differences.
package limit

import (
	"math"
	"sync"
	"time"
)

// Algorithm computes a concurrency limit from request samples.
// Implementations are safe for concurrent use.
type Algorithm interface {
	// Update records one finished request. rtt is its latency, inflight is
	// how many requests were in flight when it started (including itself),
	// and dropped reports a failure that signals overload (a timeout or a
	// rejection by a dependency).
	Update(rtt time.Duration, inflight int, dropped bool)
	// Limit returns the current limit, always >= 1.
	Limit() int
	// Name identifies the algorithm in metrics and results.
	Name() string
}

// Fixed is a static concurrency limit. It is the baseline the adaptive
// algorithms are measured against.
type Fixed struct{ N int }

func (f *Fixed) Update(time.Duration, int, bool) {}
func (f *Fixed) Limit() int                      { return max(1, f.N) }
func (f *Fixed) Name() string                    { return "fixed" }

// clamp bounds v to [lo, hi].
func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// AIMD grows the limit by one while the server is using at least half of
// it and cuts it by BackoffRatio on a drop or a request slower than Timeout.
type AIMD struct {
	mu           sync.Mutex
	limit        float64
	MinLimit     int
	MaxLimit     int
	BackoffRatio float64
	Timeout      time.Duration
}

// NewAIMD returns AIMD with the reference defaults: initial 20, min 20,
// max 200, backoff 0.9, timeout 5 s. Callers usually override Timeout and
// the bounds.
func NewAIMD(initial int) *AIMD {
	return &AIMD{limit: float64(initial), MinLimit: 20, MaxLimit: 200, BackoffRatio: 0.9, Timeout: 5 * time.Second}
}

func (a *AIMD) Name() string { return "aimd" }

func (a *AIMD) Limit() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return max(1, int(a.limit))
}

func (a *AIMD) Update(rtt time.Duration, inflight int, dropped bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if dropped || rtt > a.Timeout {
		a.limit = math.Max(float64(a.MinLimit), math.Floor(a.limit*a.BackoffRatio))
		return
	}
	if float64(inflight)*2 >= a.limit {
		a.limit = math.Min(float64(a.MaxLimit), a.limit+1)
	}
}

// Vegas estimates the queue at the server from the ratio of the minimum
// observed latency (rtt_noload) to the current latency, the way TCP Vegas
// estimates packets queued in the network:
//
//	queue = limit * (1 - rtt_noload/rtt)
//
// It grows the limit while the estimated queue is small and shrinks it once
// the queue passes beta. rtt_noload is re-probed now and then because a
// minimum learned under light load goes stale when the workload changes.
type Vegas struct {
	mu             sync.Mutex
	limit          float64
	rttNoLoad      time.Duration
	probeCount     int
	probeJitter    float64
	rnd            func() float64
	MaxLimit       int
	Smoothing      float64
	ProbeMultipler int
}

// NewVegas returns Vegas with the reference defaults: max 1000,
// smoothing 1.0, probe every 30*limit samples (jittered 0.5x to 1x).
func NewVegas(initial int) *Vegas {
	v := &Vegas{limit: float64(initial), MaxLimit: 1000, Smoothing: 1.0, ProbeMultipler: 30, rnd: fastRand}
	v.resetProbeJitter()
	return v
}

func (v *Vegas) Name() string { return "vegas" }

func (v *Vegas) resetProbeJitter() { v.probeJitter = 0.5 + v.rnd()*0.5 }

func (v *Vegas) Limit() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return max(1, int(v.limit))
}

func log10Root(limit float64) float64 { return math.Max(1, math.Log10(limit)) }

func (v *Vegas) Update(rtt time.Duration, inflight int, dropped bool) {
	if rtt <= 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.probeCount++
	if float64(v.probeCount) >= v.probeJitter*float64(v.ProbeMultipler)*v.limit {
		v.resetProbeJitter()
		v.probeCount = 0
		v.rttNoLoad = rtt
		return
	}
	if v.rttNoLoad == 0 || rtt < v.rttNoLoad {
		v.rttNoLoad = rtt
		return
	}
	est := v.limit
	queue := math.Ceil(est * (1 - float64(v.rttNoLoad)/float64(rtt)))
	l := log10Root(est)
	alpha, beta, threshold := 3*l, 6*l, l
	var next float64
	switch {
	case dropped:
		next = est - l
	case float64(inflight)*2 < est:
		return // app limited: the limit was not the constraint
	case queue <= threshold:
		next = est + beta
	case queue < alpha:
		next = est + l
	case queue > beta:
		next = est - l
	default:
		return
	}
	next = clamp(next, 1, float64(v.MaxLimit))
	v.limit = (1-v.Smoothing)*est + v.Smoothing*next
}

// Gradient2 compares a short-term latency (the last sample) against a
// long-term exponential average and scales the limit by their ratio:
//
//	gradient = clamp(tolerance * long / short, 0.5, 1)
//	limit    = limit * gradient + queueSize
//
// The long average tracks drift, so unlike Vegas it does not need a true
// minimum latency. When latency recovers after a long overload, the long
// average is pulled down faster so the limit can grow again.
type Gradient2 struct {
	mu        sync.Mutex
	limit     float64
	long      expAvg
	MinLimit  int
	MaxLimit  int
	Smoothing float64
	Tolerance float64
	QueueSize func(limit float64) float64
}

// NewGradient2 returns Gradient2 with the reference defaults: min 20,
// max 200, smoothing 0.2, tolerance 1.5, long window 600 samples
// (10 warmup), queue size 4.
func NewGradient2(initial int) *Gradient2 {
	return &Gradient2{
		limit: float64(initial), long: newExpAvg(600, 10),
		MinLimit: 20, MaxLimit: 200, Smoothing: 0.2, Tolerance: 1.5,
		QueueSize: func(float64) float64 { return 4 },
	}
}

func (g *Gradient2) Name() string { return "gradient2" }

func (g *Gradient2) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return max(1, int(g.limit))
}

func (g *Gradient2) Update(rtt time.Duration, inflight int, dropped bool) {
	if rtt <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	short := float64(rtt)
	long := g.long.add(short)
	if long/short > 2 {
		long = g.long.scale(0.95)
	}
	if float64(inflight) < g.limit/2 {
		return // app limited
	}
	gradient := clamp(g.Tolerance*long/short, 0.5, 1.0)
	next := g.limit*gradient + g.QueueSize(g.limit)
	next = g.limit*(1-g.Smoothing) + next*g.Smoothing
	g.limit = clamp(next, float64(g.MinLimit), float64(g.MaxLimit))
}

// expAvg is an exponential moving average that starts as a plain mean for
// its first warmup samples, as in the reference implementation.
type expAvg struct {
	window, warmup int
	count          int
	value          float64
}

func newExpAvg(window, warmup int) expAvg { return expAvg{window: window, warmup: warmup} }

func (e *expAvg) add(x float64) float64 {
	if e.count < e.warmup {
		e.count++
		e.value += (x - e.value) / float64(e.count)
	} else {
		f := 2.0 / float64(e.window+1)
		e.value = e.value*(1-f) + x*f
	}
	return e.value
}

func (e *expAvg) scale(f float64) float64 { e.value *= f; return e.value }

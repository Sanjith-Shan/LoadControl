package limit

import (
	"math"
	"testing"
	"time"

	"github.com/Sanjith-Shan/LoadControl/internal/leakcheck"
	"pgregory.net/rapid"
)

func TestMain(m *testing.M) { leakcheck.Main(m) }

// drawAlgorithm builds a randomly configured adaptive algorithm and returns
// its lower and upper bounds.
func drawAlgorithm(t *rapid.T) (Algorithm, int, int) {
	minL := rapid.IntRange(1, 50).Draw(t, "min")
	maxL := rapid.IntRange(minL, 1000).Draw(t, "max")
	initial := rapid.IntRange(minL, maxL).Draw(t, "initial")
	switch rapid.SampledFrom([]string{"aimd", "vegas", "gradient2"}).Draw(t, "alg") {
	case "aimd":
		a := NewAIMD(initial)
		a.MinLimit, a.MaxLimit = minL, maxL
		a.BackoffRatio = rapid.Float64Range(0.1, 0.99).Draw(t, "backoff")
		a.Timeout = time.Duration(rapid.Int64Range(1, int64(time.Second)).Draw(t, "timeout"))
		return a, minL, maxL
	case "vegas":
		v := NewVegas(initial)
		v.MaxLimit = maxL
		v.Smoothing = rapid.Float64Range(0.01, 1).Draw(t, "smoothing")
		v.ProbeMultipler = rapid.IntRange(1, 30).Draw(t, "probe")
		j := rapid.Float64Range(0, 1).Draw(t, "jitter")
		v.rnd = func() float64 { return j }
		v.resetProbeJitter()
		return v, 1, maxL
	default:
		g := NewGradient2(initial)
		g.MinLimit, g.MaxLimit = minL, maxL
		g.Smoothing = rapid.Float64Range(0.01, 1).Draw(t, "smoothing")
		g.Tolerance = rapid.Float64Range(1, 3).Draw(t, "tolerance")
		q := rapid.Float64Range(0, 10).Draw(t, "queue")
		g.QueueSize = func(float64) float64 { return q }
		return g, minL, maxL
	}
}

// Property (a): the limit stays within the configured bounds for any
// sample sequence.
func TestAlgorithmBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a, lo, hi := drawAlgorithm(t)
		n := rapid.IntRange(1, 400).Draw(t, "n")
		for i := 0; i < n; i++ {
			rtt := time.Duration(rapid.Int64Range(-1, int64(5*time.Second)).Draw(t, "rtt"))
			inflight := rapid.IntRange(0, 2*hi).Draw(t, "inflight")
			a.Update(rtt, inflight, rapid.Bool().Draw(t, "dropped"))
			if l := a.Limit(); l < lo || l > hi || l < 1 {
				t.Fatalf("%s: limit %d outside [%d,%d] after %d samples", a.Name(), l, lo, hi, i+1)
			}
		}
	})
}

// Property (d): AIMD never grows on a drop and never shrinks on a success
// that used at least half the limit.
func TestAIMDMonotone(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		minL := rapid.IntRange(1, 50).Draw(t, "min")
		maxL := rapid.IntRange(minL, 500).Draw(t, "max")
		a := NewAIMD(rapid.IntRange(minL, maxL).Draw(t, "initial"))
		a.MinLimit, a.MaxLimit, a.Timeout = minL, maxL, 100*time.Millisecond
		for i := rapid.IntRange(1, 300).Draw(t, "n"); i > 0; i-- {
			before := a.limit
			rtt := time.Duration(rapid.Int64Range(1, int64(200*time.Millisecond)).Draw(t, "rtt"))
			inflight := rapid.IntRange(0, 2*maxL).Draw(t, "inflight")
			dropped := rapid.Bool().Draw(t, "dropped")
			a.Update(rtt, inflight, dropped)
			switch {
			case dropped || rtt > a.Timeout:
				if a.limit > before {
					t.Fatalf("drop raised limit %v -> %v", before, a.limit)
				}
			case float64(inflight)*2 >= before:
				if a.limit < before {
					t.Fatalf("busy success lowered limit %v -> %v", before, a.limit)
				}
			default:
				if a.limit != before {
					t.Fatalf("app-limited success changed limit %v -> %v", before, a.limit)
				}
			}
		}
	})
}

// run feeds n samples at rtt with the algorithm fully used.
func run(a Algorithm, n int, rtt func(i int) time.Duration) {
	for i := 0; i < n; i++ {
		a.Update(rtt(i), a.Limit(), false)
	}
}

func constRTT(d time.Duration) func(int) time.Duration { return func(int) time.Duration { return d } }

// rising grows latency 1% per sample from base.
func rising(base time.Duration) func(int) time.Duration {
	return func(i int) time.Duration { return time.Duration(float64(base) * math.Pow(1.01, float64(i))) }
}

// Property (e): Gradient2 backs off to its floor while latency keeps rising
// and climbs back once it recovers.
func TestGradient2Convergence(t *testing.T) {
	g := NewGradient2(50)
	g.MinLimit, g.MaxLimit = 10, 200
	run(g, 400, constRTT(10*time.Millisecond))
	if l := g.Limit(); l != 200 {
		t.Fatalf("steady latency: limit %d, want max 200", l)
	}
	prev := g.Limit()
	for i := 0; i < 400; i++ {
		g.Update(rising(10*time.Millisecond)(i), g.Limit(), false)
		if l := g.Limit(); i > 100 && l > prev {
			t.Fatalf("sample %d: limit rose %d -> %d under rising latency", i, prev, l)
		}
		prev = g.Limit()
	}
	if l := g.Limit(); l != 10 {
		t.Fatalf("rising latency: limit %d, want min 10", l)
	}
	run(g, 400, constRTT(10*time.Millisecond))
	if l := g.Limit(); l != 200 {
		t.Fatalf("recovered latency: limit %d, want 200", l)
	}
}

func TestVegasConvergence(t *testing.T) {
	v := NewVegas(20)
	v.MaxLimit = 500
	v.ProbeMultipler = 1 << 20 // no re-probe during the test
	run(v, 200, constRTT(10*time.Millisecond))
	high := v.Limit()
	if high < 200 {
		t.Fatalf("steady latency: limit %d, want growth to >= 200", high)
	}
	run(v, 2000, func(i int) time.Duration { return min(rising(10*time.Millisecond)(i), 100*time.Millisecond) })
	low := v.Limit()
	if low > high/10 {
		t.Fatalf("latency 10x: limit %d, want < %d", low, high/10)
	}
	run(v, 200, constRTT(10*time.Millisecond))
	if l := v.Limit(); l < high {
		t.Fatalf("recovered latency: limit %d, want >= %d", l, high)
	}
}

// App-limited samples (inflight below half the limit) must not move the
// delay-based algorithms.
func TestAppLimited(t *testing.T) {
	for _, a := range []Algorithm{NewVegas(100), NewGradient2(100)} {
		a.Update(10*time.Millisecond, 100, false)
		before := a.Limit()
		for i := 0; i < 100; i++ {
			a.Update(time.Duration(i+1)*time.Second, 10, false)
		}
		if a.Limit() != before {
			t.Errorf("%s: app-limited samples moved limit %d -> %d", a.Name(), before, a.Limit())
		}
	}
}

func TestVegasProbeResetsBaseline(t *testing.T) {
	v := NewVegas(10)
	v.ProbeMultipler = 1
	v.rnd = func() float64 { return 1 }
	v.resetProbeJitter()
	v.Update(10*time.Millisecond, 10, false)
	for i := 0; i < 100 && v.rttNoLoad != 50*time.Millisecond; i++ {
		v.Update(50*time.Millisecond, v.Limit(), false)
	}
	if v.rttNoLoad != 50*time.Millisecond {
		t.Fatalf("rtt_noload %v never re-probed to 50ms", v.rttNoLoad)
	}
}

func BenchmarkUpdate(b *testing.B) {
	for _, mk := range []func() Algorithm{
		func() Algorithm { return &Fixed{N: 100} },
		func() Algorithm { return NewAIMD(100) },
		func() Algorithm { return NewVegas(100) },
		func() Algorithm { return NewGradient2(100) },
	} {
		a := mk()
		b.Run(a.Name(), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				a.Update(time.Duration(10+i%7)*time.Millisecond, 60+i%40, i%50 == 0)
			}
		})
	}
}

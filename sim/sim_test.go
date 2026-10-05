package sim

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"testing"

	"github.com/Sanjith-Shan/LoadControl/limit"
)

func run(t *testing.T, config string) (*Sim, *Result) {
	t.Helper()
	p := Default()
	if err := p.ApplyConfig(config); err != nil {
		t.Fatal(err)
	}
	s, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	return s, s.Run()
}

func mean(secs []Second, from, to int) float64 {
	var g float64
	for _, s := range secs[from:to] {
		g += float64(s.Goodput)
	}
	return g / float64(to-from)
}

// Every policy that draws random numbers is on: throttle, Vegas probing,
// backoff jitter, ratio budget, DAGOR.
const busy = "load.x=2,duration_s=15,LIMIT=vegas,THROTTLE=2,RETRY=ratio,BACKOFF_MS=5,PER_TRY_TIMEOUT_MS=200," +
	"DAGOR=wait,QUEUE_WAIT_MS=20,TIER_SHARES=1,0.8,0.6,user.retries=2,user.backoff_ms=10,FRONTEND_LIMIT=gradient2"

func TestDeterminism(t *testing.T) {
	enc := func(r *Result) string {
		r.Summary.WallMS = 0
		b, _ := json.Marshal(r)
		return string(b)
	}
	_, a := run(t, busy)
	_, b := run(t, busy)
	if enc(a) != enc(b) {
		t.Fatal("same seed, different output")
	}
	_, c := run(t, busy+",seed=2")
	if enc(a) == enc(c) {
		t.Fatal("different seeds, same output")
	}
}

// Every user request ends exactly once, and after draining every limiter
// slot, pooled connection and CPU job is returned.
func TestConservation(t *testing.T) {
	for _, cfg := range []string{
		"load.x=3,duration_s=10,user.retries=2",
		busy,
		"load.x=0.7,duration_s=20,user.retries=2,RETRY=naive,PER_TRY_TIMEOUT_MS=300,flush.at=5,flush.dur=5,slow.at=5,slow.dur=5,slow.extra_ms=100,mongo.pool=10",
		"load.x=3,duration_s=10,LIMIT=gradient2,DEADLINE=on,FRONTEND_DEFAULT_TIMEOUT_MS=800,RATELIMIT=3000,max_concurrency.search=20,cancel_propagation=false",
		"load.x=2,duration_s=10,cpu=fcfs,LIMIT=aimd,QUEUE_WAIT_MS=50",
	} {
		s, r := run(t, cfg)
		o := r.Summary.Outcomes
		if o[0] == 0 || o[0] != o[1]+o[2]+o[3] || s.outstanding != 0 {
			t.Errorf("%s: offered %d != success %d + failed %d + timeout %d (outstanding %d)", cfg, o[0], o[1], o[2], o[3], s.outstanding)
		}
		for s.step() {
		}
		for _, v := range s.svc {
			if v.lim != nil && (v.lim.inflight != 0 || len(v.lim.queue) != 0) {
				t.Errorf("%s: %s limiter left inflight %d queue %d", cfg, v.name, v.lim.inflight, len(v.lim.queue))
			}
			if v.poolUsed != 0 || v.running != 0 {
				t.Errorf("%s: %s left pool %d running %d", cfg, v.name, v.poolUsed, v.running)
			}
		}
		if s.cpu.load() != 0 {
			t.Errorf("%s: CPU left %d jobs", cfg, s.cpu.load())
		}
	}
}

// At 3x capacity an uncontrolled system collapses; Gradient2 with
// deadline dropping holds goodput near capacity.
func TestOverload(t *testing.T) {
	_, none := run(t, "load.x=3,duration_s=30")
	_, g2 := run(t, "load.x=3,duration_s=30,LIMIT=gradient2,DEADLINE=on")
	t.Logf("goodput/capacity at 3x: none %.2f, gradient2+deadline %.2f", none.Summary.GoodputX, g2.Summary.GoodputX)
	if none.Summary.GoodputX > 0.3 {
		t.Errorf("no control kept %.2f of capacity at 3x", none.Summary.GoodputX)
	}
	if g2.Summary.GoodputX < 0.85 {
		t.Errorf("gradient2+deadline kept only %.2f of capacity at 3x", g2.Summary.GoodputX)
	}
}

// At 0.7x a 10 s cold cache plus a MongoDB slowdown tips naive retries at
// every hop into a state that outlives the trigger; a retry budget, the
// one-layer rule and a limiter recover.
func TestMetastable(t *testing.T) {
	const base = "load.x=0.7,duration_s=80,user.retries=2,RETRY_ATTEMPTS=3,PER_TRY_TIMEOUT_MS=300,"
	const trig = "flush.at=20,flush.dur=10,slow.at=20,slow.dur=10,slow.extra_ms=100,"
	_, quiet := run(t, base+"RETRY=naive")
	_, naive := run(t, base+trig+"RETRY=naive")
	_, safe := run(t, base+trig+"RETRY=budget,ONE_LAYER=on,LIMIT=gradient2,DEADLINE=on,user.honor_no_retry=true")
	pre := mean(naive.Seconds, 5, 20)
	q, n, s := mean(quiet.Seconds, 40, 80), mean(naive.Seconds, 40, 80), mean(safe.Seconds, 40, 80)
	t.Logf("goodput before %.0f; 10-50 s after the trigger: no trigger %.0f, naive %.0f, budget+one-layer+limiter %.0f (recovery %v s)",
		pre, q, n, s, deref(safe.Summary.RecoveryS))
	if q < 0.9*pre {
		t.Errorf("naive retries degrade without a trigger: %.0f vs %.0f", q, pre)
	}
	if n > 0.3*pre || naive.Summary.RecoveryS != nil {
		t.Errorf("naive retries recovered after the trigger: %.0f vs %.0f before", n, pre)
	}
	if s < 0.9*pre || safe.Summary.RecoveryS == nil || *safe.Summary.RecoveryS > 10 {
		t.Errorf("controlled system did not recover: %.0f vs %.0f before, recovery %v", s, pre, safe.Summary.RecoveryS)
	}
}

func deref(p *float64) any {
	if p == nil {
		return "never"
	}
	return *p
}

// The event limiter makes the same decisions as limit.Limiter on random
// non-blocking acquire/release sequences with tier shares.
func TestLimiterMatchesLibrary(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	shares := []float64{1, 0.8, 0.5}
	for trial := range 50 {
		n := 1 + rng.IntN(30)
		real := limit.NewLimiter(&limit.Fixed{N: n}, limit.Options{Shares: shares})
		s := &Sim{}
		ours := &limiter{alg: &limit.Fixed{N: n}, shares: shares}
		var rt []*limit.Token
		var ot []*token
		for op := range 500 {
			if len(rt) > 0 && rng.IntN(2) == 0 {
				i := rng.IntN(len(rt))
				rt[i].Success()
				s.release(ot[i], true, false)
				rt, ot = append(rt[:i], rt[i+1:]...), append(ot[:i], ot[i+1:]...)
				continue
			}
			tier := rng.IntN(4)
			tok, err := real.Acquire(context.Background(), tier)
			var got *token
			s.acquire(ours, tier, &ctx{}, func(t *token) { got = t })
			if (err == nil) != (got != nil) {
				t.Fatalf("trial %d op %d tier %d limit %d inflight %d: library admitted=%v, sim admitted=%v",
					trial, op, tier, n, ours.inflight, err == nil, got != nil)
			}
			if got != nil {
				rt, ot = append(rt, tok), append(ot, got)
			}
			if real.Inflight() != ours.inflight {
				t.Fatalf("inflight %d vs %d", real.Inflight(), ours.inflight)
			}
		}
	}
}

// Waiters are served by tier then FIFO, time out after MaxWait, and leave
// when their context ends.
func TestLimiterQueue(t *testing.T) {
	s := &Sim{}
	l := &limiter{alg: &limit.Fixed{N: 1}, maxWait: ms(10), maxQueue: 10}
	var first *token
	s.acquire(l, 1, &ctx{}, func(t *token) { first = t })
	var order []string
	add := func(name string, tier int, c *ctx) {
		s.acquire(l, tier, c, func(t *token) {
			if t == nil {
				order = append(order, name+":rejected")
				return
			}
			order = append(order, name)
			s.release(t, true, false)
		})
	}
	gone := &ctx{}
	add("d1", 1, &ctx{})
	add("s1", 2, &ctx{})
	add("c1", 0, gone)
	add("d2", 1, &ctx{})
	add("c2", 0, &ctx{})
	s.cancel(gone, Canceled)
	s.now = ms(5)
	s.release(first, true, false)
	for s.step() {
	}
	want := []string{"c1:rejected", "c2", "d1", "d2", "s1"}
	if len(order) != len(want) {
		t.Fatalf("order %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order %v, want %v", order, want)
		}
	}
	// A waiter that never gets a slot is rejected at MaxWait.
	t0 := s.now
	s.acquire(l, 1, &ctx{}, func(t *token) { first = t })
	var late *token
	rejected := false
	s.acquire(l, 1, &ctx{}, func(t *token) { late, rejected = t, t == nil })
	for s.step() {
	}
	if !rejected || late != nil || s.now != t0+ms(10) {
		t.Fatalf("waiter not rejected at MaxWait (now %d)", s.now)
	}
}

func BenchmarkRun5000(b *testing.B) {
	for range b.N {
		p := Default()
		p.ApplyConfig("load.rps=5000,LIMIT=gradient2,DEADLINE=on")
		Run(p)
	}
}

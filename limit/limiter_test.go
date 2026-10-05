package limit

import (
	"context"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// stepAlg is an Algorithm whose limit the test sets. A sampled release
// (Success or Dropped) applies the pending next limit, the way a real
// algorithm moves only on samples.
type stepAlg struct {
	mu          sync.Mutex
	limit, next int
}

func (s *stepAlg) Update(time.Duration, int, bool) {
	s.mu.Lock()
	s.limit = s.next
	s.mu.Unlock()
}
func (s *stepAlg) Limit() int   { s.mu.Lock(); defer s.mu.Unlock(); return s.limit }
func (s *stepAlg) Name() string { return "step" }
func (s *stepAlg) setNext(n int) {
	s.mu.Lock()
	s.next = n
	s.mu.Unlock()
}

type acquired struct {
	tok *Token
	err error
}

type pending struct {
	tier   int
	seq    int
	cancel context.CancelFunc
	res    chan acquired
}

// limiterModel is the reference model for the Limiter state machine: a
// count of admitted requests and a queue ordered by (tier, arrival).
type limiterModel struct {
	l        *Limiter
	alg      *stepAlg
	shares   []float64
	wait     bool
	maxQueue int

	inflight int
	held     []*Token
	queue    []*pending
	seq      int
}

func (m *limiterModel) capFor(tier int) int {
	s := 1.0
	if len(m.shares) > 0 {
		s = m.shares[min(tier, len(m.shares)-1)]
	}
	return max(1, int(s*float64(m.alg.Limit())))
}

func waitUntil(t *rapid.T, what string, cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Microsecond)
	}
}

func recv(t *rapid.T, p *pending) acquired {
	select {
	case r := <-p.res:
		return r
	case <-time.After(5 * time.Second):
		t.Fatalf("waiter tier %d seq %d never returned", p.tier, p.seq)
		return acquired{}
	}
}

func (m *limiterModel) acquire(t *rapid.T) {
	tier := rapid.IntRange(0, 3).Draw(t, "tier")
	limit := m.alg.Limit()
	direct := m.inflight < m.capFor(tier) && (len(m.queue) == 0 || m.queue[0].tier > tier)
	maxQ := m.maxQueue
	if maxQ == 0 {
		maxQ = 4 * limit
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.seq++
	p := &pending{tier: tier, seq: m.seq, cancel: cancel, res: make(chan acquired, 1)}
	go func() {
		tok, err := m.l.Acquire(ctx, tier)
		p.res <- acquired{tok, err}
	}()
	switch {
	case direct:
		r := recv(t, p)
		cancel()
		if r.err != nil {
			t.Fatalf("tier %d refused with inflight %d < cap %d", tier, m.inflight, m.capFor(tier))
		}
		if share := m.capFor(tier); m.inflight >= share || m.inflight >= limit {
			t.Fatalf("admitted at inflight %d, cap %d, limit %d", m.inflight, share, limit)
		}
		m.inflight++
		m.held = append(m.held, r.tok)
	case !m.wait || len(m.queue) >= maxQ:
		r := recv(t, p)
		cancel()
		if r.err != ErrLimitExceeded {
			t.Fatalf("tier %d admitted with inflight %d, cap %d, queue %d", tier, m.inflight, m.capFor(tier), len(m.queue))
		}
	default:
		waitUntil(t, "enqueue", func() bool { return m.l.QueueLen() == len(m.queue)+1 })
		if len(p.res) != 0 {
			t.Fatalf("tier %d should have queued", tier)
		}
		i := sort.Search(len(m.queue), func(i int) bool { return m.queue[i].tier > tier })
		m.queue = append(m.queue[:i], append([]*pending{p}, m.queue[i:]...)...)
	}
}

func (m *limiterModel) release(t *rapid.T) {
	if len(m.held) == 0 {
		t.Skip("nothing held")
	}
	i := rapid.IntRange(0, len(m.held)-1).Draw(t, "token")
	kind := rapid.SampledFrom([]string{"success", "dropped", "ignore"}).Draw(t, "kind")
	m.alg.setNext(rapid.IntRange(1, 8).Draw(t, "next"))
	tok := m.held[i]
	m.held = append(m.held[:i], m.held[i+1:]...)
	m.releaseToken(t, tok, kind)
}

func (m *limiterModel) releaseToken(t *rapid.T, tok *Token, kind string) {
	switch kind {
	case "success":
		tok.Success()
	case "dropped":
		tok.Dropped()
	default:
		tok.Ignore()
	}
	m.inflight--
	// Waiters are granted head first while the head's tier fits.
	for len(m.queue) > 0 && m.inflight < m.capFor(m.queue[0].tier) {
		p := m.queue[0]
		m.queue = m.queue[1:]
		r := recv(t, p)
		p.cancel()
		if r.err != nil {
			t.Fatalf("waiter tier %d seq %d not granted: %v", p.tier, p.seq, r.err)
		}
		m.inflight++
		if m.inflight > m.alg.Limit() {
			t.Fatalf("granted to inflight %d above limit %d", m.inflight, m.alg.Limit())
		}
		m.held = append(m.held, r.tok)
	}
	for _, p := range m.queue {
		if len(p.res) != 0 {
			t.Fatalf("waiter tier %d seq %d granted out of (tier, FIFO) order", p.tier, p.seq)
		}
	}
}

func (m *limiterModel) cancelWaiter(t *rapid.T) {
	if len(m.queue) == 0 {
		t.Skip("no waiters")
	}
	i := rapid.IntRange(0, len(m.queue)-1).Draw(t, "waiter")
	p := m.queue[i]
	m.queue = append(m.queue[:i], m.queue[i+1:]...)
	p.cancel()
	if r := recv(t, p); r.err != ErrLimitExceeded {
		t.Fatalf("cancelled waiter got %v", r.err)
	}
}

func (m *limiterModel) check(t *rapid.T) {
	if got := m.l.Inflight(); got != m.inflight || got < 0 {
		t.Fatalf("Inflight() = %d, model %d", got, m.inflight)
	}
	if got := m.l.QueueLen(); got != len(m.queue) {
		t.Fatalf("QueueLen() = %d, model %d", got, len(m.queue))
	}
}

// drain releases everything; no waiter may be left behind with free slots.
func (m *limiterModel) drain(t *rapid.T) {
	for len(m.held) > 0 {
		tok := m.held[0]
		m.held = m.held[1:]
		m.releaseToken(t, tok, "success")
	}
	if len(m.queue) != 0 || m.l.Inflight() != 0 || m.l.QueueLen() != 0 {
		t.Fatalf("after release: inflight %d, queue %d, model queue %d", m.l.Inflight(), m.l.QueueLen(), len(m.queue))
	}
}

// Properties (b) and (c): admission respects the limit and tier shares,
// inflight matches the model, waiters are granted in (tier, FIFO) order,
// and no slot is lost.
func TestLimiterStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 4).Draw(t, "tiers")
		shares := make([]float64, n)
		for i := range shares {
			shares[i] = rapid.Float64Range(0.05, 1).Draw(t, "share")
		}
		sort.Sort(sort.Reverse(sort.Float64Slice(shares)))
		lim := rapid.IntRange(1, 8).Draw(t, "limit")
		alg := &stepAlg{limit: lim, next: lim}
		m := &limiterModel{alg: alg, shares: shares,
			wait: rapid.Bool().Draw(t, "wait"), maxQueue: rapid.IntRange(0, 6).Draw(t, "maxQueue")}
		opts := Options{Shares: shares, MaxQueue: m.maxQueue}
		if m.wait {
			opts.MaxWait = time.Hour
		}
		m.l = NewLimiter(alg, opts)
		defer func() {
			for _, p := range m.queue {
				p.cancel()
				<-p.res
			}
		}()
		t.Repeat(map[string]func(*rapid.T){
			"acquire": m.acquire,
			"release": m.release,
			"cancel":  m.cancelWaiter,
			"":        m.check,
		})
		m.drain(t)
	})
}

// TestLimiterConcurrent hammers a limiter from many goroutines with a real
// algorithm and checks that nothing leaks.
func TestLimiterConcurrent(t *testing.T) {
	alg := NewGradient2(20)
	alg.MinLimit, alg.MaxLimit = 2, 40
	l := NewLimiter(alg, Options{Shares: []float64{1, 0.8, 0.5}, MaxWait: time.Millisecond})
	var held, peak atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed))
			for i := 0; i < 300; i++ {
				tok, err := l.Acquire(context.Background(), r.IntN(3))
				if err != nil {
					continue
				}
				h := held.Add(1)
				for p := peak.Load(); h > p && !peak.CompareAndSwap(p, h); p = peak.Load() {
				}
				time.Sleep(time.Duration(r.IntN(100)) * time.Microsecond)
				held.Add(-1)
				switch r.IntN(3) {
				case 0:
					tok.Success()
				case 1:
					tok.Dropped()
				default:
					tok.Ignore()
				}
			}
		}(uint64(g))
	}
	wg.Wait()
	if l.Inflight() != 0 || l.QueueLen() != 0 {
		t.Fatalf("inflight %d queue %d after all released", l.Inflight(), l.QueueLen())
	}
	if peak.Load() > 40 {
		t.Fatalf("peak concurrency %d above max limit 40", peak.Load())
	}
}

func TestTokenReleaseIdempotent(t *testing.T) {
	l := NewLimiter(&Fixed{N: 1}, Options{})
	tok, err := l.Acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	tok.Success()
	tok.Success()
	tok.Ignore()
	if l.Inflight() != 0 {
		t.Fatalf("double release: inflight %d", l.Inflight())
	}
}

func BenchmarkLimiterAcquire(b *testing.B) {
	l := NewLimiter(NewGradient2(100), Options{Shares: []float64{1, 0.9, 0.7}})
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if tok, err := l.Acquire(ctx, 1); err == nil {
				tok.Success()
			}
		}
	})
}

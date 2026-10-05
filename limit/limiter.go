package limit

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

func fastRand() float64 { return rand.Float64() }

// ErrLimitExceeded is returned when a request is refused because the
// concurrency limit (or its tier's share of it) is used up and it could not
// get a slot within the queue timeout.
var ErrLimitExceeded = errors.New("loadcontrol: concurrency limit exceeded")

// Options configure a Limiter. The zero value is a single-tier limiter that
// rejects immediately when full.
type Options struct {
	// Shares[t] is the fraction of the limit that requests of tier t may
	// occupy. Tier 0 is the most important. A request of tier t is admitted
	// only while inflight < Shares[t]*limit, so lower tiers are refused
	// first as the server fills up and the top tier keeps headroom.
	// Missing tiers use the last entry; nil means every tier may use the
	// whole limit.
	Shares []float64
	// MaxWait lets a request that finds the limiter full wait this long for
	// a slot. Waiters are served highest tier first, FIFO within a tier.
	// Zero rejects at once.
	MaxWait time.Duration
	// MaxQueue bounds the number of waiters. Zero means 4*limit.
	MaxQueue int
	// Now is the clock; nil uses time.Now. Tests inject a fake one.
	Now func() time.Time
}

// Limiter enforces an Algorithm's limit on in-flight requests.
type Limiter struct {
	alg  Algorithm
	opts Options
	now  func() time.Time

	mu       sync.Mutex
	inflight int
	queue    []*waiter // sorted by (tier, seq)
	seq      uint64

	// Observer, if set, is called (outside the lock) for every admission
	// decision. Metrics hang off it.
	Observer func(Event)
}

// Event describes one admission decision or completion.
type Event struct {
	Kind     EventKind
	Tier     int
	Wait     time.Duration // time spent queued before the decision
	RTT      time.Duration // for completions
	Inflight int
	Limit    int
}

type EventKind int

const (
	Admitted EventKind = iota
	Rejected
	Completed
	DroppedSample
)

type waiter struct {
	tier    int
	seq     uint64
	ready   chan struct{}
	granted bool
	removed bool
}

// NewLimiter wraps alg.
func NewLimiter(alg Algorithm, opts Options) *Limiter {
	l := &Limiter{alg: alg, opts: opts, now: opts.Now}
	if l.now == nil {
		l.now = time.Now
	}
	return l
}

// Algorithm returns the wrapped algorithm.
func (l *Limiter) Algorithm() Algorithm { return l.alg }

// Inflight returns the number of admitted, unfinished requests.
func (l *Limiter) Inflight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight
}

// QueueLen returns the number of waiting requests.
func (l *Limiter) QueueLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queue)
}

func (l *Limiter) share(tier int) float64 {
	s := l.opts.Shares
	if len(s) == 0 {
		return 1
	}
	if tier < 0 {
		tier = 0
	}
	if tier >= len(s) {
		return s[len(s)-1]
	}
	return s[tier]
}

// capFor is the in-flight count below which a tier may be admitted.
func (l *Limiter) capFor(tier, limit int) int {
	c := int(l.share(tier) * float64(limit))
	if c < 1 {
		c = 1
	}
	return c
}

// Token is held by an admitted request. Exactly one of Success, Dropped or
// Ignore must be called when the request finishes.
type Token struct {
	l        *Limiter
	start    time.Time
	inflight int
	tier     int
	Wait     time.Duration
	done     bool
}

// Acquire admits a request of the given tier or returns ErrLimitExceeded.
// If Options.MaxWait > 0 it may block up to MaxWait (or until ctx ends).
func (l *Limiter) Acquire(ctx context.Context, tier int) (*Token, error) {
	start := l.now()
	l.mu.Lock()
	limit := l.alg.Limit()
	if l.inflight < l.capFor(tier, limit) && !l.higherWaiting(tier) {
		l.inflight++
		t := &Token{l: l, start: start, inflight: l.inflight, tier: tier}
		inf := l.inflight
		l.mu.Unlock()
		l.emit(Event{Kind: Admitted, Tier: tier, Inflight: inf, Limit: limit})
		return t, nil
	}
	maxQ := l.opts.MaxQueue
	if maxQ == 0 {
		maxQ = 4 * limit
	}
	if l.opts.MaxWait <= 0 || len(l.queue) >= maxQ || ctx.Err() != nil {
		inf := l.inflight
		l.mu.Unlock()
		l.emit(Event{Kind: Rejected, Tier: tier, Inflight: inf, Limit: limit})
		return nil, ErrLimitExceeded
	}
	l.seq++
	w := &waiter{tier: tier, seq: l.seq, ready: make(chan struct{})}
	l.insert(w)
	l.mu.Unlock()

	timer := time.NewTimer(l.opts.MaxWait)
	defer timer.Stop()
	select {
	case <-w.ready:
	case <-timer.C:
	case <-ctx.Done():
	}
	l.mu.Lock()
	if !w.granted {
		l.remove(w)
		inf := l.inflight
		l.mu.Unlock()
		l.emit(Event{Kind: Rejected, Tier: tier, Wait: l.now().Sub(start), Inflight: inf, Limit: limit})
		return nil, ErrLimitExceeded
	}
	inf := l.inflight
	l.mu.Unlock()
	now := l.now()
	t := &Token{l: l, start: now, inflight: inf, tier: tier, Wait: now.Sub(start)}
	l.emit(Event{Kind: Admitted, Tier: tier, Wait: t.Wait, Inflight: inf, Limit: limit})
	return t, nil
}

// higherWaiting reports whether a waiter of the same or a more important
// tier is queued, in which case a new arrival must not jump ahead of it.
func (l *Limiter) higherWaiting(tier int) bool {
	return len(l.queue) > 0 && l.queue[0].tier <= tier
}

func (l *Limiter) insert(w *waiter) {
	i := len(l.queue)
	for i > 0 && l.queue[i-1].tier > w.tier {
		i--
	}
	l.queue = append(l.queue, nil)
	copy(l.queue[i+1:], l.queue[i:])
	l.queue[i] = w
}

func (l *Limiter) remove(w *waiter) {
	if w.removed {
		return
	}
	for i, q := range l.queue {
		if q == w {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			break
		}
	}
	w.removed = true
}

// grant hands free slots to queued waiters, most important first. Called
// with mu held.
func (l *Limiter) grant() {
	limit := l.alg.Limit()
	for len(l.queue) > 0 {
		w := l.queue[0]
		if l.inflight >= l.capFor(w.tier, limit) {
			return
		}
		l.queue = l.queue[1:]
		w.removed = true
		w.granted = true
		l.inflight++
		close(w.ready)
	}
}

func (t *Token) release(sample bool, dropped bool) {
	if t.done {
		return
	}
	t.done = true
	l := t.l
	rtt := l.now().Sub(t.start)
	if sample {
		l.alg.Update(rtt, t.inflight, dropped)
	}
	l.mu.Lock()
	l.inflight--
	l.grant()
	inf := l.inflight
	l.mu.Unlock()
	kind := Completed
	if dropped {
		kind = DroppedSample
	}
	l.emit(Event{Kind: kind, Tier: t.tier, RTT: rtt, Inflight: inf, Limit: l.alg.Limit()})
}

// Success records a normal completion and its latency.
func (t *Token) Success() { t.release(true, false) }

// Dropped records a completion that signals overload (timeout, dependency
// rejection). Loss-based algorithms back off on it.
func (t *Token) Dropped() { t.release(true, true) }

// Ignore releases the slot without teaching the algorithm anything, for
// requests whose latency says nothing about load (client errors, cancels).
func (t *Token) Ignore() { t.release(false, false) }

func (l *Limiter) emit(e Event) {
	if l.Observer != nil {
		l.Observer(e)
	}
}

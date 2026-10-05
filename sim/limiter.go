package sim

import (
	"time"

	"github.com/Sanjith-Shan/LoadControl/limit"
)

// limiter is limit.Limiter's admission and queueing re-expressed as events
// (the real one blocks a goroutine on a channel and a timer). The limit
// itself comes from the library's Algorithm, fed the same samples.
// Semantics kept identical: tier t is admitted directly only while
// inflight < max(1, int(share[t]*limit)) and no same-or-higher tier
// waiter is queued; waiters are ordered by tier then FIFO, leave after
// MaxWait or when their context ends, and are granted on release.
type limiter struct {
	alg      limit.Algorithm
	shares   []float64
	maxWait  int64
	maxQueue int
	inflight int
	queue    []*waiter
	seq      uint64
}

type waiter struct {
	tier    int
	seq     uint64
	start   int64
	settled bool
	cb      func(*token)
}

type token struct {
	l        *limiter
	start    int64
	inflight int
	wait     int64
	done     bool
}

func (l *limiter) share(tier int) float64 {
	if len(l.shares) == 0 {
		return 1
	}
	return l.shares[min(max(tier, 0), len(l.shares)-1)]
}

func (l *limiter) capFor(tier, lim int) int { return max(1, int(l.share(tier)*float64(lim))) }

// acquire calls cb with a token, or with nil on rejection.
func (s *Sim) acquire(l *limiter, tier int, c *ctx, cb func(*token)) {
	lim := l.alg.Limit()
	if l.inflight < l.capFor(tier, lim) && !(len(l.queue) > 0 && l.queue[0].tier <= tier) {
		l.inflight++
		cb(&token{l: l, start: s.now, inflight: l.inflight})
		return
	}
	maxQ := l.maxQueue
	if maxQ == 0 {
		maxQ = 4 * lim
	}
	if l.maxWait <= 0 || len(l.queue) >= maxQ || c.err != OK {
		cb(nil)
		return
	}
	l.seq++
	w := &waiter{tier: tier, seq: l.seq, start: s.now, cb: cb}
	i := len(l.queue)
	for i > 0 && l.queue[i-1].tier > tier {
		i--
	}
	l.queue = append(l.queue, nil)
	copy(l.queue[i+1:], l.queue[i:])
	l.queue[i] = w
	give := func() {
		if w.settled {
			return
		}
		w.settled = true
		for i, q := range l.queue {
			if q == w {
				l.queue = append(l.queue[:i], l.queue[i+1:]...)
				break
			}
		}
		s.post(func() { cb(nil) })
	}
	s.at(s.now+l.maxWait, give)
	c.onDone(give)
}

// grant hands free slots to waiters, most important first.
func (s *Sim) grant(l *limiter) {
	lim := l.alg.Limit()
	for len(l.queue) > 0 {
		w := l.queue[0]
		if l.inflight >= l.capFor(w.tier, lim) {
			return
		}
		l.queue = l.queue[1:]
		w.settled = true
		l.inflight++
		t := &token{l: l, start: s.now, inflight: l.inflight, wait: s.now - w.start}
		s.post(func() { w.cb(t) })
	}
}

// release ends a token: sample feeds the algorithm (Success or Dropped),
// no sample is Ignore.
func (s *Sim) release(t *token, sample, dropped bool) {
	if t.done {
		return
	}
	t.done = true
	if sample {
		t.l.alg.Update(time.Duration(s.now-t.start), t.inflight, dropped)
	}
	t.l.inflight--
	s.grant(t.l)
}

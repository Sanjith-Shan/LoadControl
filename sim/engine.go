package sim

import (
	"math"
	"time"
)

// The engine: a virtual clock in nanoseconds, a timer heap, a FIFO of
// work runnable at the current instant, and one processor-sharing CPU.
// Everything runs on one goroutine, so a run is a pure function of its
// params and seed.

type event struct {
	t   int64
	seq uint64
	fn  func()
}

type eventQ []event

func (q eventQ) less(i, j int) bool {
	return q[i].t < q[j].t || (q[i].t == q[j].t && q[i].seq < q[j].seq)
}

func (q *eventQ) push(e event) {
	*q = append(*q, e)
	h := *q
	for i := len(h) - 1; i > 0; {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func (q *eventQ) pop() event {
	h := *q
	top := h[0]
	n := len(h) - 1
	h[0] = h[n]
	h[n] = event{}
	h = h[:n]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && h.less(l, m) {
			m = l
		}
		if r < n && h.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
	*q = h
	return top
}

// epoch anchors the virtual clock handed to library components, so their
// wall-clock bucketing (throttle, ratio budget) behaves as in production.
var epoch = time.Unix(1_700_000_000, 0)

func (s *Sim) clock() time.Time { return epoch.Add(time.Duration(s.now)) }

func ms(x float64) int64 { return int64(x * 1e6) }

// at runs fn at time t (now if t is in the past).
func (s *Sim) at(t int64, fn func()) {
	if t <= s.now {
		s.post(fn)
		return
	}
	s.seq++
	s.q.push(event{t, s.seq, fn})
}

// post runs fn at the current instant, after work already posted. It is
// how callbacks avoid re-entering the code that triggered them, the way a
// goroutine woken by a channel runs later than the closer.
func (s *Sim) post(fn func()) { s.ready = append(s.ready, fn) }

// step runs the next instant's work; false when nothing is left.
func (s *Sim) step() bool {
	if s.rh < len(s.ready) {
		for s.rh < len(s.ready) {
			fn := s.ready[s.rh]
			s.ready[s.rh] = nil
			s.rh++
			s.events++
			fn()
		}
		s.ready, s.rh = s.ready[:0], 0
		return true
	}
	ct := s.cpu.next()
	if len(s.q) == 0 && ct == math.MaxInt64 {
		return false
	}
	if len(s.q) > 0 && s.q[0].t <= ct {
		e := s.q.pop()
		s.now = e.t
		s.events++
		e.fn()
		return true
	}
	s.now = ct
	s.cpu.complete(s)
	return true
}

// work draws a CPU demand with the given mean in ms, as nanoseconds.
func (s *Sim) work(mean float64) float64 {
	if mean <= 0 {
		return 0
	}
	m := mean * 1e6
	switch s.P.Dist {
	case "const":
		return m
	case "lognormal":
		sig := math.Sqrt(math.Log(1 + s.P.CV*s.P.CV))
		return m * math.Exp(sig*s.rSvc.NormFloat64()-sig*sig/2)
	}
	return m * s.rSvc.ExpFloat64()
}

// Status codes, named after gRPC's. HTTP statuses map onto them:
// 503 = ResourceExhausted, 504 = DeadlineExceeded, 500 = Internal.
type code uint8

const (
	OK code = iota
	Canceled
	DeadlineExceeded
	ResourceExhausted
	Unavailable
	Internal
)

// ctx mirrors context.Context: an effective deadline, an error once done,
// and cancellation that flows to children. hooks are the select arms of
// whoever is blocked on Done().
type ctx struct {
	deadline int64 // 0 = none
	err      code
	asCancel bool // a parent's end arrives as Canceled (an HTTP client hanging up)
	kids     []*ctx
	hooks    []func()
}

// newCtx derives a context with an absolute deadline (0 = none). inherit
// copies the parent's deadline (gRPC propagates it; plain HTTP does not,
// the server only sees the disconnect).
func (s *Sim) newCtx(parent *ctx, deadline int64, inherit bool) *ctx {
	c := &ctx{}
	if parent != nil {
		if inherit {
			c.deadline = parent.deadline
		} else {
			c.asCancel = true
		}
		if parent.err != OK {
			c.err = parent.err
			if c.asCancel {
				c.err = Canceled
			}
			return c
		}
		parent.kids = append(parent.kids, c)
	}
	if deadline > 0 && (c.deadline == 0 || deadline < c.deadline) {
		c.deadline = deadline
		s.at(deadline, func() { s.cancel(c, DeadlineExceeded) })
	}
	return c
}

func (s *Sim) cancel(c *ctx, err code) {
	if c.err != OK {
		return
	}
	c.err = err
	for _, h := range c.hooks {
		h()
	}
	for _, k := range c.kids {
		e := err
		if k.asCancel {
			e = Canceled
		}
		s.cancel(k, e)
	}
	c.hooks, c.kids = nil, nil
}

// onDone registers fn to run (synchronously) when c ends.
func (c *ctx) onDone(fn func()) { c.hooks = append(c.hooks, fn) }

// Package throttle implements client-side adaptive throttling from the
// Google SRE book, chapter 21 ("Handling Overload"). Each client tracks, over
// a trailing window, how many requests it attempted and how many the
// backend accepted, and rejects new requests locally with probability
//
//	max(0, (requests - K*accepts) / (requests + 1))
//
// so once the backend starts refusing work, most of the refused traffic
// never leaves the client. K trades wasted backend work for false local
// rejections: K=2 lets the backend see about twice what it accepts.
package throttle

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Throttle is one client's view of one backend. Safe for concurrent use.
type Throttle struct {
	K       float64
	buckets []bucket
	width   time.Duration
	now     func() time.Time
	rnd     func() float64

	mu sync.Mutex
}

type bucket struct {
	start             int64 // bucket index in units of width
	requests, accepts float64
}

// New returns a throttle over window, split into 1-second buckets (the
// book uses two minutes of history).
func New(k float64, window time.Duration, now func() time.Time) *Throttle {
	if now == nil {
		now = time.Now
	}
	n := int(window / time.Second)
	if n < 1 {
		n = 1
	}
	return &Throttle{K: k, buckets: make([]bucket, n), width: time.Second, now: now, rnd: rand.Float64}
}

func (t *Throttle) cur() *bucket {
	idx := t.now().UnixNano() / int64(t.width)
	b := &t.buckets[idx%int64(len(t.buckets))]
	if b.start != idx {
		*b = bucket{start: idx}
	}
	return b
}

func (t *Throttle) totals() (req, acc float64) {
	idx := t.now().UnixNano() / int64(t.width)
	for i := range t.buckets {
		b := &t.buckets[i]
		if idx-b.start < int64(len(t.buckets)) {
			req += b.requests
			acc += b.accepts
		}
	}
	return
}

// RejectProbability returns the current local rejection probability.
func (t *Throttle) RejectProbability() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	req, acc := t.totals()
	return max(0, (req-t.K*acc)/(req+1))
}

// Allow decides whether to send a request. It counts the request either
// way, as the formula requires: locally rejected requests still count as
// requests, which is what keeps the probability up while the backend is
// down.
func (t *Throttle) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	req, acc := t.totals()
	p := max(0, (req-t.K*acc)/(req+1))
	t.cur().requests++
	return t.rnd() >= p
}

// Accepted records that the backend accepted a request (any response that
// is not an overload rejection, including application errors).
func (t *Throttle) Accepted() {
	t.mu.Lock()
	t.cur().accepts++
	t.mu.Unlock()
}

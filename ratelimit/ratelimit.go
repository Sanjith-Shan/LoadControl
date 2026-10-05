// Package ratelimit is a static token bucket, the simplest overload
// defense and the baseline the adaptive pieces are compared against.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket admits up to Rate requests per second with bursts up to Burst.
type Bucket struct {
	Rate  float64
	Burst float64
	now   func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func New(rate, burst float64, now func() time.Time) *Bucket {
	if now == nil {
		now = time.Now
	}
	return &Bucket{Rate: rate, Burst: burst, now: now, tokens: burst, last: now()}
}

// Allow takes one token if available.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.Rate
	if b.tokens > b.Burst {
		b.tokens = b.Burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

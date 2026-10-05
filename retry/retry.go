// Package retry holds the transport-independent parts of safe retrying: a
// retry budget, backoff, server pushback, and the "retry at one layer"
// rule.
//
// Budget is the token bucket from gRPC's retry design (gRFC A6, "retry
// throttling"): every failed attempt costs one token, every success earns
// TokenRatio tokens, and retries are only allowed while more than half of
// MaxTokens remain. When a backend is failing most requests, tokens drain
// and retries stop, so retries can add at most a bounded fraction of load
// instead of multiplying it.
//
// The one-layer rule: when a request fails because a dependency further
// down was overloaded, only the caller directly in front of the overloaded
// service may retry. Every layer above sees the failure marked as
// "do not retry". Without this, a chain of n services that each retry r
// times sends up to r^n requests to the bottom one.
package retry

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Budgeter decides whether a retry may be sent.
type Budgeter interface {
	// OnRequest records an original (non-retry) request.
	OnRequest()
	// OnResult records one attempt's outcome. failed means a retryable
	// failure (overload, unavailable, timeout).
	OnResult(failed bool)
	// AllowRetry reports whether a retry may be sent now, and spends from
	// the budget if so.
	AllowRetry() bool
}

// Budget is the gRFC A6 token bucket. Safe for concurrent use.
type Budget struct {
	MaxTokens  float64
	TokenRatio float64

	mu     sync.Mutex
	tokens float64
}

// NewBudget returns a full bucket. gRFC A6's example uses maxTokens 10 and
// tokenRatio 0.1, which allows retries while fewer than about one request
// in eleven fails.
func NewBudget(maxTokens, tokenRatio float64) *Budget {
	return &Budget{MaxTokens: maxTokens, TokenRatio: tokenRatio, tokens: maxTokens}
}

func (b *Budget) OnRequest() {}

func (b *Budget) OnResult(failed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if failed {
		b.tokens = math.Max(0, b.tokens-1)
	} else {
		b.tokens = math.Min(b.MaxTokens, b.tokens+b.TokenRatio)
	}
}

func (b *Budget) AllowRetry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens > b.MaxTokens/2
}

// Tokens returns the current token count.
func (b *Budget) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}

// Unlimited allows every retry. It is the "naive retries at every hop"
// baseline.
type Unlimited struct{}

func (Unlimited) OnRequest()       {}
func (Unlimited) OnResult(bool)    {}
func (Unlimited) AllowRetry() bool { return true }

// RatioBudget allows retries up to Ratio of original requests over a
// sliding window, plus MinPerSec so low-traffic clients can still retry.
// This is the other common budget shape (Finagle, Envoy's retry budget);
// it is included so the experiments can compare the two.
type RatioBudget struct {
	Ratio     float64
	MinPerSec float64
	window    time.Duration
	now       func() time.Time

	mu      sync.Mutex
	reqs    []float64
	retries []float64
	stamp   []int64
}

func NewRatioBudget(ratio, minPerSec float64, window time.Duration, now func() time.Time) *RatioBudget {
	if now == nil {
		now = time.Now
	}
	n := max(1, int(window/time.Second))
	return &RatioBudget{Ratio: ratio, MinPerSec: minPerSec, window: window, now: now,
		reqs: make([]float64, n), retries: make([]float64, n), stamp: make([]int64, n)}
}

func (r *RatioBudget) slot() int {
	sec := r.now().Unix()
	i := int(sec % int64(len(r.stamp)))
	if r.stamp[i] != sec {
		r.stamp[i], r.reqs[i], r.retries[i] = sec, 0, 0
	}
	return i
}

func (r *RatioBudget) sums() (req, ret float64) {
	sec := r.now().Unix()
	for i := range r.stamp {
		if sec-r.stamp[i] < int64(len(r.stamp)) {
			req += r.reqs[i]
			ret += r.retries[i]
		}
	}
	return
}

func (r *RatioBudget) OnRequest() {
	r.mu.Lock()
	r.reqs[r.slot()]++
	r.mu.Unlock()
}

func (r *RatioBudget) OnResult(bool) {}

func (r *RatioBudget) AllowRetry() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, ret := r.sums()
	allowed := r.Ratio*req + r.MinPerSec*r.window.Seconds()
	if ret+1 > allowed {
		return false
	}
	r.retries[r.slot()]++
	return true
}

// Backoff is exponential backoff with full jitter, gRFC A6 style:
// the n-th retry waits uniform(0, min(Initial*Multiplier^(n-1), Max)).
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

// Delay returns the wait before retry number n (1-based).
func (b Backoff) Delay(n int) time.Duration {
	if b.Initial <= 0 {
		return 0
	}
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(n-1))
	if b.Max > 0 && d > float64(b.Max) {
		d = float64(b.Max)
	}
	return time.Duration(rand.Float64() * d)
}

// Header names used on the wire. gRPC metadata keys are lower case; HTTP
// canonicalizes them.
const (
	// PushbackHeader is gRFC A6's server pushback: a non-negative integer
	// tells the client how many milliseconds to wait before retrying; a
	// negative or malformed value tells it not to retry at all.
	PushbackHeader = "grpc-retry-pushback-ms"
	// NoRetryHeader marks a failure that already went through a retrying
	// layer, so callers above must not retry it again.
	NoRetryHeader = "x-lc-no-retry"
	// ShedHeader marks a rejection made by LoadControl's admission control
	// (as opposed to an application error), with the reason as value.
	ShedHeader = "x-lc-shed"
	// AttemptHeader carries the attempt number (1 = original) so servers
	// can count retries per hop.
	AttemptHeader = "x-lc-attempt"
)

// Pushback interprets a pushback header value. ok=false means the server
// said not to retry.
func Pushback(v string) (d time.Duration, present, ok bool) {
	if v == "" {
		return 0, false, true
	}
	var n int64
	for i, c := range v {
		if c == '-' && i == 0 {
			return 0, true, false
		}
		if c < '0' || c > '9' {
			return 0, true, false
		}
		n = n*10 + int64(c-'0')
		if n > math.MaxInt32 {
			return 0, true, false
		}
	}
	return time.Duration(n) * time.Millisecond, true, true
}

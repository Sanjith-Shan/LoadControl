// Package loadcontrol is overload control for Go services: adaptive
// concurrency limits, priority-aware admission, deadline propagation with
// dead-work dropping, client-side adaptive throttling, retry budgets and
// retries at one layer only.
//
// This package holds the transport-independent server and client cores.
// The lcgrpc and lchttp packages adapt them to gRPC-Go interceptors and
// net/http middleware. Every piece is optional so experiments can switch
// them on one at a time.
package loadcontrol

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/ratelimit"
)

// ServerConfig selects the server-side pieces. Nil or zero disables each.
type ServerConfig struct {
	Name string

	// Limiter enforces an adaptive (or fixed) concurrency limit.
	Limiter *limit.Limiter
	// Dagor gates requests by compound priority before the limiter.
	Dagor *priority.Dagor
	// DagorSignal picks the queueing-delay signal fed to Dagor:
	// "wait" (limiter queue wait, default) or "sched" (Go scheduler latency).
	DagorSignal string
	// RateLimit is a static token bucket, the "rate limit at measured
	// capacity" baseline.
	RateLimit *ratelimit.Bucket

	// DropExpired rejects a request whose deadline has passed, or has less
	// than MinBudget left, before doing any work.
	DropExpired bool
	MinBudget   time.Duration
	// DefaultTimeout gives requests that arrive without a deadline one, so
	// that deadlines exist to propagate. Used at the edge.
	DefaultTimeout time.Duration

	// Pushback is sent with overload rejections as gRFC A6 server
	// pushback: >0 asks the client to wait that long before retrying,
	// <0 tells it not to retry. Zero sends nothing.
	Pushback time.Duration

	// OneLayer marks failures caused by a failed downstream call as
	// "do not retry" for this server's callers.
	OneLayer bool

	Metrics *Metrics
}

// Server is the admission pipeline. Order per request:
// deadline check, static rate limit, DAGOR level, concurrency limiter.
type Server struct {
	cfg   ServerConfig
	sched *priority.SchedLatency
	stop  chan struct{}
	once  sync.Once

	// Counters for tests and the experiment harness.
	Admitted, ShedDeadline, ShedLimit, ShedDagor, ShedRate atomic.Int64
}

// Rejection says why a request was refused.
type Rejection struct {
	Reason   string // "deadline", "limit", "dagor", "ratelimit"
	Pushback time.Duration
}

func (r *Rejection) Error() string { return "loadcontrol: shed (" + r.Reason + ")" }

// IsOverload reports whether the rejection means "server busy" (as opposed
// to "your deadline is gone").
func (r *Rejection) IsOverload() bool { return r.Reason != "deadline" }

// NewServer builds the pipeline. If a Dagor is configured with the
// scheduler-latency signal, a background ticker samples it.
func NewServer(cfg ServerConfig) *Server {
	s := &Server{cfg: cfg, stop: make(chan struct{})}
	if cfg.Dagor != nil {
		if cfg.DagorSignal == "sched" {
			s.sched = priority.NewSchedLatency()
		}
		go s.tick()
	}
	return s
}

// Close stops background work.
func (s *Server) Close() { s.once.Do(func() { close(s.stop) }) }

// Config returns the configuration.
func (s *Server) Config() ServerConfig { return s.cfg }

func (s *Server) tick() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			if s.sched != nil {
				if m, n := s.sched.Mean(); n > 0 {
					s.cfg.Dagor.ObserveDelay(m)
				}
			}
			s.cfg.Dagor.Tick()
			if s.cfg.Metrics != nil {
				s.cfg.Metrics.DagorLevel.WithLabelValues(s.cfg.Name).Set(float64(s.cfg.Dagor.LevelKey()))
			}
		}
	}
}

type callStateKey struct{}

// callState tracks, per inbound request, whether an outbound call made
// while serving it failed after the client side gave up. The one-layer
// rule uses it.
type callState struct{ downstreamFailed atomic.Bool }

func withCallState(ctx context.Context) (context.Context, *callState) {
	cs := &callState{}
	return context.WithValue(ctx, callStateKey{}, cs), cs
}

// MarkDownstreamFailure records that an outbound call failed for good.
// Client adapters call it.
func MarkDownstreamFailure(ctx context.Context) {
	if cs, ok := ctx.Value(callStateKey{}).(*callState); ok {
		cs.downstreamFailed.Store(true)
	}
}

// Ticket is an admitted request. Call Done exactly once.
type Ticket struct {
	s     *Server
	tok   *limit.Token
	info  priority.Info
	start time.Time
	cs    *callState
	cancel context.CancelFunc
}

// Outcome classifies how an admitted request ended, for the limiter.
type Outcome int

const (
	OK       Outcome = iota // finished normally (including app errors)
	Overload                // timed out or failed because a dependency was overloaded
	Ignore                  // cancelled by the caller or otherwise uninformative
)

// Admit runs admission for a request whose priority is already on ctx (or
// in info). It returns the context to serve the request with, a ticket,
// or a rejection.
func (s *Server) Admit(ctx context.Context, info priority.Info) (context.Context, *Ticket, *Rejection) {
	c := s.cfg
	tier := strconv.Itoa(int(info.Tier))
	var cancel context.CancelFunc
	if c.DefaultTimeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			ctx, cancel = context.WithTimeout(ctx, c.DefaultTimeout)
		}
	}
	reject := func(reason string, counter *atomic.Int64) (context.Context, *Ticket, *Rejection) {
		counter.Add(1)
		if cancel != nil {
			cancel()
		}
		if c.Metrics != nil {
			c.Metrics.Requests.WithLabelValues(c.Name, tier, "shed_"+reason).Inc()
		}
		r := &Rejection{Reason: reason}
		if reason != "deadline" {
			r.Pushback = c.Pushback
		}
		return ctx, nil, r
	}
	if c.DropExpired {
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < c.MinBudget {
			return reject("deadline", &s.ShedDeadline)
		}
		if ctx.Err() != nil {
			return reject("deadline", &s.ShedDeadline)
		}
	}
	if c.RateLimit != nil && !c.RateLimit.Allow() {
		return reject("ratelimit", &s.ShedRate)
	}
	if c.Dagor != nil && !c.Dagor.Admit(info) {
		return reject("dagor", &s.ShedDagor)
	}
	var tok *limit.Token
	if c.Limiter != nil {
		var err error
		tok, err = c.Limiter.Acquire(ctx, int(info.Tier))
		if err != nil {
			if c.DropExpired && ctx.Err() != nil {
				return reject("deadline", &s.ShedDeadline)
			}
			return reject("limit", &s.ShedLimit)
		}
		if c.Dagor != nil && s.sched == nil {
			c.Dagor.ObserveDelay(tok.Wait)
		}
		if c.Metrics != nil {
			c.Metrics.QueueWait.WithLabelValues(c.Name).Observe(tok.Wait.Seconds())
		}
		// A request that waited past its deadline is dead work.
		if c.DropExpired && ctx.Err() != nil {
			tok.Ignore()
			return reject("deadline", &s.ShedDeadline)
		}
	}
	s.Admitted.Add(1)
	if c.Metrics != nil {
		c.Metrics.Requests.WithLabelValues(c.Name, tier, "admitted").Inc()
		if c.Limiter != nil {
			c.Metrics.Limit.WithLabelValues(c.Name).Set(float64(c.Limiter.Algorithm().Limit()))
			c.Metrics.Inflight.WithLabelValues(c.Name).Set(float64(c.Limiter.Inflight()))
		}
	}
	ctx = priority.WithInfo(ctx, info)
	ctx, cs := withCallState(ctx)
	return ctx, &Ticket{s: s, tok: tok, info: info, start: time.Now(), cs: cs, cancel: cancel}, nil
}

// Done finishes the request. It returns true if the failure should be
// marked "do not retry" for the caller (one-layer rule): the handler
// failed after one of its own downstream calls failed for good.
func (t *Ticket) Done(o Outcome, failed bool) (noRetry bool) {
	if t.tok != nil {
		switch o {
		case OK:
			t.tok.Success()
		case Overload:
			t.tok.Dropped()
		default:
			t.tok.Ignore()
		}
	}
	c := t.s.cfg
	if c.Metrics != nil {
		c.Metrics.Latency.WithLabelValues(c.Name, strconv.Itoa(int(t.info.Tier))).Observe(time.Since(t.start).Seconds())
	}
	if t.cancel != nil {
		t.cancel()
	}
	return c.OneLayer && failed && t.cs.downstreamFailed.Load()
}

// DownstreamFailed reports whether an outbound call failed for good while
// serving this request.
func (t *Ticket) DownstreamFailed() bool { return t.cs.downstreamFailed.Load() }

// IsDeadline reports whether err is a context deadline or cancellation.
func IsDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

package loadcontrol

import (
	"context"
	"errors"
	"time"

	"github.com/Sanjith-Shan/LoadControl/retry"
	"github.com/Sanjith-Shan/LoadControl/throttle"
)

// ClientConfig selects the client-side pieces for calls to one target.
type ClientConfig struct {
	Service string // the calling service, for metric labels
	Target  string // the callee

	// Throttle is the SRE client-side adaptive throttle. Nil disables it.
	Throttle *throttle.Throttle
	// ThrottleOnFailure counts every retryable failure (timeouts,
	// unavailable), not only overload rejections, as "not accepted". This
	// lets the throttle work without server-side shedding, the way proxy
	// admission-control filters apply the same formula.
	ThrottleOnFailure bool
	// Budget gates retries. Nil means no retries at all.
	Budget retry.Budgeter
	// MaxAttempts is the total attempts per call including the first.
	MaxAttempts int
	Backoff     retry.Backoff
	// PerTryTimeout bounds each attempt separately, as proxies such as
	// Envoy do. Zero means attempts share the caller's deadline.
	PerTryTimeout time.Duration
	// HonorPushback obeys server pushback (wait, or do not retry).
	HonorPushback bool
	// HonorNoRetry obeys the one-layer marker from the callee.
	HonorNoRetry bool

	Metrics *Metrics
}

// ErrThrottled is returned when the adaptive throttle rejects a call
// locally. Adapters translate it to their transport's overload error.
var ErrThrottled = errors.New("loadcontrol: rejected locally by adaptive throttle")

// Attempt is what a transport adapter reports about one try.
type Attempt struct {
	Err        error
	Retryable  bool   // transport-level judgement: overload, unavailable, timeout
	Overloaded bool   // the callee rejected for overload (does not count as an accept)
	NoRetry    bool   // callee marked the failure "do not retry"
	Pushback   string // raw pushback header value, if any
}

// Client runs the attempt loop for calls to one target.
type Client struct{ cfg ClientConfig }

func NewClient(cfg ClientConfig) *Client {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &Client{cfg: cfg}
}

func (c *Client) Config() ClientConfig { return c.cfg }

// Do calls try until it succeeds, the failure is not retryable, the
// budget refuses, or attempts run out. try gets the attempt number
// (1 = original) and a context bounded by PerTryTimeout if set.
func (c *Client) Do(ctx context.Context, try func(ctx context.Context, attempt int) Attempt) error {
	cfg := c.cfg
	m := cfg.Metrics
	if cfg.Budget != nil {
		cfg.Budget.OnRequest()
	}
	var last Attempt
	for n := 1; ; n++ {
		if cfg.Throttle != nil && !cfg.Throttle.Allow() {
			if m != nil {
				m.ClientLocal.WithLabelValues(cfg.Service, cfg.Target, "throttle").Inc()
				m.ThrottleProb.WithLabelValues(cfg.Service, cfg.Target).Set(cfg.Throttle.RejectProbability())
			}
			if n == 1 {
				last = Attempt{Err: ErrThrottled, Overloaded: true}
			}
			break
		}
		if m != nil {
			kind := "original"
			if n > 1 {
				kind = "retry"
			}
			m.ClientAttempts.WithLabelValues(cfg.Service, cfg.Target, kind).Inc()
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		if cfg.PerTryTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, cfg.PerTryTimeout)
		}
		last = try(actx, n)
		cancel()
		if last.Err == nil {
			if cfg.Throttle != nil {
				cfg.Throttle.Accepted()
			}
			if cfg.Budget != nil {
				cfg.Budget.OnResult(false)
			}
			return nil
		}
		if cfg.Throttle != nil && !last.Overloaded && !(cfg.ThrottleOnFailure && last.Retryable) {
			cfg.Throttle.Accepted()
		}
		// gRFC A6: only retryable failures drain the bucket; other
		// failures leave it unchanged.
		if cfg.Budget != nil && last.Retryable {
			cfg.Budget.OnResult(true)
		}
		if m != nil {
			if b, ok := cfg.Budget.(*retry.Budget); ok {
				m.RetryTokens.WithLabelValues(cfg.Service, cfg.Target).Set(b.Tokens())
			}
		}
		if !last.Retryable || n >= cfg.MaxAttempts || cfg.Budget == nil || ctx.Err() != nil {
			break
		}
		if cfg.HonorNoRetry && last.NoRetry {
			if m != nil {
				m.ClientLocal.WithLabelValues(cfg.Service, cfg.Target, "no_retry").Inc()
			}
			break
		}
		wait := cfg.Backoff.Delay(n)
		if cfg.HonorPushback {
			d, present, ok := retry.Pushback(last.Pushback)
			if !ok {
				break
			}
			if present {
				wait = d
			}
		}
		if !cfg.Budget.AllowRetry() {
			if m != nil {
				m.ClientLocal.WithLabelValues(cfg.Service, cfg.Target, "budget").Inc()
			}
			break
		}
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				MarkDownstreamFailure(ctx)
				return last.Err
			}
		}
	}
	if m != nil {
		m.ClientFailures.WithLabelValues(cfg.Service, cfg.Target).Inc()
	}
	MarkDownstreamFailure(ctx)
	return last.Err
}

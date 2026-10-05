package loadcontrol

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"github.com/Sanjith-Shan/LoadControl/throttle"
	"pgregory.net/rapid"
)

var errBusy = errors.New("busy")

func busy() Attempt { return Attempt{Err: errBusy, Retryable: true, Overloaded: true} }

// count runs one Do with try returning a(n) and reports the attempts made.
func count(t *testing.T, c *Client, a func(n int) Attempt) (int, error) {
	t.Helper()
	n := 0
	err := c.Do(context.Background(), func(_ context.Context, attempt int) Attempt {
		n++
		if attempt != n {
			t.Fatalf("attempt number %d, want %d", attempt, n)
		}
		return a(n)
	})
	return n, err
}

func TestClientAttempts(t *testing.T) {
	naive := func(cfg ClientConfig) *Client {
		cfg.Budget, cfg.MaxAttempts = retry.Unlimited{}, 3
		return NewClient(cfg)
	}
	cases := []struct {
		name string
		c    *Client
		try  func(n int) Attempt
		want int
		ok   bool
	}{
		{"retries up to MaxAttempts", naive(ClientConfig{}), func(int) Attempt { return busy() }, 3, false},
		{"success stops", naive(ClientConfig{}), func(n int) Attempt {
			if n == 2 {
				return Attempt{}
			}
			return busy()
		}, 2, true},
		{"not retryable", naive(ClientConfig{}), func(int) Attempt { return Attempt{Err: errBusy} }, 1, false},
		{"no budget no retries", NewClient(ClientConfig{MaxAttempts: 3}), func(int) Attempt { return busy() }, 1, false},
		{"negative pushback", naive(ClientConfig{HonorPushback: true}), func(int) Attempt {
			a := busy()
			a.Pushback = "-1"
			return a
		}, 1, false},
		{"pushback ignored when not honored", naive(ClientConfig{}), func(int) Attempt {
			a := busy()
			a.Pushback = "-1"
			return a
		}, 3, false},
		{"no-retry honored", naive(ClientConfig{HonorNoRetry: true}), func(int) Attempt {
			a := busy()
			a.NoRetry = true
			return a
		}, 1, false},
		{"no-retry ignored", naive(ClientConfig{}), func(int) Attempt {
			a := busy()
			a.NoRetry = true
			return a
		}, 3, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := count(t, c.c, c.try)
			if n != c.want || (err == nil) != c.ok {
				t.Fatalf("attempts %d err %v; want %d ok=%v", n, err, c.want, c.ok)
			}
		})
	}
}

func TestClientPushbackWaits(t *testing.T) {
	c := NewClient(ClientConfig{Budget: retry.Unlimited{}, MaxAttempts: 2, HonorPushback: true})
	var at []time.Time
	count(t, c, func(int) Attempt {
		at = append(at, time.Now())
		a := busy()
		a.Pushback = "60"
		return a
	})
	if len(at) != 2 || at[1].Sub(at[0]) < 60*time.Millisecond {
		t.Fatalf("attempts %d, gap %v; want 2 attempts 60ms apart", len(at), at[len(at)-1].Sub(at[0]))
	}
}

func TestClientContextEndsBackoff(t *testing.T) {
	c := NewClient(ClientConfig{Budget: retry.Unlimited{}, MaxAttempts: 5, HonorPushback: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	n := 0
	start := time.Now()
	err := c.Do(ctx, func(context.Context, int) Attempt {
		n++
		a := busy()
		a.Pushback = "10000"
		return a
	})
	if n != 1 || err != errBusy || time.Since(start) > 5*time.Second {
		t.Fatalf("attempts %d err %v after %v", n, err, time.Since(start))
	}
}

func TestClientPerTryTimeout(t *testing.T) {
	c := NewClient(ClientConfig{Budget: retry.Unlimited{}, MaxAttempts: 2, PerTryTimeout: 10 * time.Millisecond})
	n := 0
	c.Do(context.Background(), func(ctx context.Context, _ int) Attempt {
		n++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("attempt context has no deadline")
		}
		<-ctx.Done()
		return Attempt{Err: ctx.Err(), Retryable: true}
	})
	if n != 2 {
		t.Fatalf("attempts %d, want 2", n)
	}
}

// With every attempt failing, a gRFC A6 bucket of 10 tokens allows 4
// retries in total: tokens go 10, 9, ..., and retrying needs more than 5.
func TestClientBudgetStopsRetries(t *testing.T) {
	c := NewClient(ClientConfig{Budget: retry.NewBudget(10, 0.1), MaxAttempts: 100})
	total := 0
	for i := 0; i < 20; i++ {
		n, _ := count(t, c, func(int) Attempt { return busy() })
		total += n - 1
	}
	if total != 4 {
		t.Fatalf("%d retries over 20 failing calls, want 4", total)
	}
}

// Property (f), long run: let phi = max(0, tokens - Max/2). A retry is only
// allowed after a failure that left phi > 0, so that failure lowered phi by
// exactly 1; only the attempt that ends a call can earn TokenRatio, so phi
// gains at most TokenRatio per call. Hence retries <= Max/2 + TokenRatio*calls
// whatever the failure pattern.
func TestClientBudgetBound(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxT := float64(rapid.IntRange(1, 20).Draw(t, "max"))
		ratio := rapid.Float64Range(0.01, 1).Draw(t, "ratio")
		f := rapid.Float64Range(0, 1).Draw(t, "failure fraction")
		r := rand.New(rand.NewPCG(rapid.Uint64().Draw(t, "seed"), 1))
		c := NewClient(ClientConfig{Budget: retry.NewBudget(maxT, ratio), MaxAttempts: rapid.IntRange(2, 10).Draw(t, "attempts")})
		calls := rapid.IntRange(1, 2000).Draw(t, "calls")
		retries := 0
		for i := 0; i < calls; i++ {
			c.Do(context.Background(), func(_ context.Context, n int) Attempt {
				if n > 1 {
					retries++
				}
				if r.Float64() < f {
					return busy()
				}
				return Attempt{}
			})
		}
		if bound := maxT/2 + ratio*float64(calls); float64(retries) > bound {
			t.Fatalf("%d retries over %d calls > bound %v (f=%v)", retries, calls, bound, f)
		}
	})
}

func TestClientThrottled(t *testing.T) {
	c := NewClient(ClientConfig{Throttle: throttle.New(2, time.Minute, nil)})
	for i := 0; i < 1000; i++ {
		called := false
		err := c.Do(context.Background(), func(context.Context, int) Attempt {
			called = true
			return busy()
		})
		if err == ErrThrottled {
			if called {
				t.Fatal("throttled call still reached the backend")
			}
			return
		}
	}
	t.Fatal("throttle never rejected against a backend that refuses everything")
}

func TestClientMarksDownstreamFailure(t *testing.T) {
	s := NewServer(ServerConfig{OneLayer: true})
	c := NewClient(ClientConfig{Budget: retry.Unlimited{}, MaxAttempts: 2})
	for _, fail := range []bool{false, true} {
		ctx, tk, rej := s.Admit(context.Background(), priority.Info{})
		if rej != nil {
			t.Fatal(rej)
		}
		err := c.Do(ctx, func(context.Context, int) Attempt {
			if fail {
				return busy()
			}
			return Attempt{}
		})
		if tk.DownstreamFailed() != fail || (err != nil) != fail {
			t.Fatalf("fail=%v: DownstreamFailed %v err %v", fail, tk.DownstreamFailed(), err)
		}
		if noRetry := tk.Done(OK, err != nil); noRetry != fail {
			t.Fatalf("fail=%v: Done noRetry %v", fail, noRetry)
		}
	}
}

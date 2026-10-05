package throttle

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newClock() *clock { return &clock{time.Unix(1_700_000_000, 0)} }

type event struct {
	sec    int64
	accept bool
}

// Property (g): the reject probability is max(0, (req - K*acc)/(req+1))
// over the requests and accepts of the trailing window, and Allow rejects
// exactly when the random draw falls below it.
func TestProbabilityMatchesFormula(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := newClock()
		k := rapid.Float64Range(1, 3).Draw(t, "K")
		window := rapid.IntRange(1, 10).Draw(t, "window")
		th := New(k, time.Duration(window)*time.Second, c.now)
		var events []event
		model := func() float64 {
			var req, acc float64
			now := c.t.Unix()
			for _, e := range events {
				if now-e.sec < int64(window) {
					if e.accept {
						acc++
					} else {
						req++
					}
				}
			}
			return math.Max(0, (req-k*acc)/(req+1))
		}
		for i := rapid.IntRange(1, 200).Draw(t, "n"); i > 0; i-- {
			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0:
				c.t = c.t.Add(time.Duration(rapid.Int64Range(0, int64(3*time.Second)).Draw(t, "advance")))
			case 1:
				r := rapid.Float64Range(0, 1).Draw(t, "rnd")
				th.rnd = func() float64 { return r }
				want := r >= model()
				if got := th.Allow(); got != want {
					t.Fatalf("Allow() = %v with rnd %v, p %v", got, r, model())
				}
				events = append(events, event{sec: c.t.Unix()})
			case 2:
				th.Accepted()
				events = append(events, event{sec: c.t.Unix(), accept: true})
			}
			if got, want := th.RejectProbability(), model(); math.Abs(got-want) > 1e-12 {
				t.Fatalf("RejectProbability() = %v, want %v", got, want)
			}
		}
	})
}

func TestWindowExpiry(t *testing.T) {
	c := newClock()
	th := New(2, 10*time.Second, c.now)
	th.rnd = func() float64 { return 1 } // always send, still counted
	for i := 0; i < 99; i++ {
		th.Allow()
	}
	if p := th.RejectProbability(); p != 0.99 {
		t.Fatalf("100%% failures: p = %v, want 0.99", p)
	}
	c.t = c.t.Add(9 * time.Second)
	if p := th.RejectProbability(); p != 0.99 {
		t.Fatalf("inside window: p = %v, want 0.99", p)
	}
	c.t = c.t.Add(time.Second)
	if p := th.RejectProbability(); p != 0 {
		t.Fatalf("after window: p = %v, want 0", p)
	}
}

// With every request accepted the throttle never rejects.
func TestHealthyBackendNeverThrottled(t *testing.T) {
	th := New(2, time.Minute, newClock().now)
	th.rnd = func() float64 { return 0 }
	for i := 0; i < 1000; i++ {
		if !th.Allow() {
			t.Fatalf("request %d throttled with a healthy backend", i)
		}
		th.Accepted()
	}
}

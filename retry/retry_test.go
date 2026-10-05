package retry

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Property (f): tokens stay in [0, MaxTokens] and a retry is allowed iff
// more than half remain.
func TestBudgetTokens(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxT := rapid.Float64Range(1, 100).Draw(t, "max")
		ratio := rapid.Float64Range(0.01, 2).Draw(t, "ratio")
		b := NewBudget(maxT, ratio)
		model := maxT
		for i := rapid.IntRange(1, 300).Draw(t, "n"); i > 0; i-- {
			if rapid.Bool().Draw(t, "failed") {
				b.OnResult(true)
				model = math.Max(0, model-1)
			} else {
				b.OnResult(false)
				model = math.Min(maxT, model+ratio)
			}
			tok := b.Tokens()
			if tok < 0 || tok > maxT || math.Abs(tok-model) > 1e-9 {
				t.Fatalf("tokens %v, model %v, max %v", tok, model, maxT)
			}
			if got, want := b.AllowRetry(), tok > maxT/2; got != want {
				t.Fatalf("AllowRetry() = %v with %v of %v tokens", got, tok, maxT)
			}
		}
	})
}

func TestUnlimited(t *testing.T) {
	var u Unlimited
	for i := 0; i < 100; i++ {
		u.OnResult(true)
	}
	if !u.AllowRetry() {
		t.Fatal("Unlimited refused a retry")
	}
}

// RatioBudget allows a retry only if the window's retries, counting it,
// stay within Ratio*requests + MinPerSec*window.
func TestRatioBudgetBound(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		now := time.Unix(1_700_000_000, 0)
		ratio := rapid.Float64Range(0, 1).Draw(t, "ratio")
		minPS := rapid.Float64Range(0, 5).Draw(t, "minPerSec")
		r := NewRatioBudget(ratio, minPS, 10*time.Second, func() time.Time { return now })
		type ev struct {
			sec   int64
			retry bool
		}
		var evs []ev
		for i := rapid.IntRange(1, 300).Draw(t, "n"); i > 0; i-- {
			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0:
				now = now.Add(time.Duration(rapid.Int64Range(0, int64(2*time.Second)).Draw(t, "advance")))
				continue
			case 1:
				r.OnRequest()
				evs = append(evs, ev{sec: now.Unix()})
				continue
			}
			if !r.AllowRetry() {
				continue
			}
			evs = append(evs, ev{now.Unix(), true})
			var req, ret float64
			for _, e := range evs {
				if now.Unix()-e.sec < 10 {
					if e.retry {
						ret++
					} else {
						req++
					}
				}
			}
			if ret > ratio*req+minPS*10 {
				t.Fatalf("%v retries for %v requests in window (ratio %v, min %v/s)", ret, req, ratio, minPS)
			}
		}
	})
}

func TestBackoffBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		b := Backoff{
			Initial:    time.Duration(rapid.Int64Range(1, int64(time.Second)).Draw(t, "initial")),
			Max:        time.Duration(rapid.Int64Range(0, int64(10*time.Second)).Draw(t, "max")),
			Multiplier: rapid.Float64Range(1, 3).Draw(t, "mult"),
		}
		n := rapid.IntRange(1, 20).Draw(t, "n")
		hi := float64(b.Initial) * math.Pow(b.Multiplier, float64(n-1))
		if b.Max > 0 {
			hi = math.Min(hi, float64(b.Max))
		}
		if d := b.Delay(n); d < 0 || float64(d) > hi {
			t.Fatalf("Delay(%d) = %v outside [0, %v]", n, d, time.Duration(hi))
		}
	})
	if d := (Backoff{}).Delay(3); d != 0 {
		t.Fatalf("zero Backoff waited %v", d)
	}
}

func TestPushback(t *testing.T) {
	for _, c := range []struct {
		in               string
		d                time.Duration
		present, retryOK bool
	}{
		{"", 0, false, true},
		{"0", 0, true, true},
		{"250", 250 * time.Millisecond, true, true},
		{"-1", 0, true, false},
		{"abc", 0, true, false},
		{"1.5", 0, true, false},
		{"99999999999", 0, true, false},
	} {
		d, present, ok := Pushback(c.in)
		if d != c.d || present != c.present || ok != c.retryOK {
			t.Errorf("Pushback(%q) = %v, %v, %v; want %v, %v, %v", c.in, d, present, ok, c.d, c.present, c.retryOK)
		}
	}
}

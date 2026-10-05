package loadcontrol

import (
	"context"
	"testing"
	"time"

	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/ratelimit"
)

func admit(s *Server, ctx context.Context, tier priority.Tier) (*Ticket, *Rejection) {
	_, t, r := s.Admit(ctx, priority.Info{Tier: tier})
	return t, r
}

func TestAdmitDeadline(t *testing.T) {
	s := NewServer(ServerConfig{DropExpired: true, MinBudget: 50 * time.Millisecond, Limiter: limit.NewLimiter(&limit.Fixed{N: 1}, limit.Options{})})
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, r := admit(s, short, 1); r == nil || r.Reason != "deadline" || r.IsOverload() {
		t.Fatalf("short budget: %v", r)
	}
	gone, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, r := admit(s, gone, 1); r == nil || r.Reason != "deadline" {
		t.Fatalf("cancelled: %v", r)
	}
	if s.ShedDeadline.Load() != 2 || s.Config().Limiter.Inflight() != 0 {
		t.Fatalf("ShedDeadline %d inflight %d", s.ShedDeadline.Load(), s.Config().Limiter.Inflight())
	}
	tk, r := admit(s, context.Background(), 1)
	if r != nil {
		t.Fatalf("no deadline: %v", r)
	}
	tk.Done(OK, false)
}

func TestAdmitLimitAndPushback(t *testing.T) {
	l := limit.NewLimiter(&limit.Fixed{N: 2}, limit.Options{})
	s := NewServer(ServerConfig{Limiter: l, Pushback: 50 * time.Millisecond, Metrics: NewMetrics(nil)})
	a, _ := admit(s, context.Background(), 1)
	b, _ := admit(s, context.Background(), 1)
	_, r := admit(s, context.Background(), 1)
	if r == nil || r.Reason != "limit" || r.Pushback != 50*time.Millisecond || !r.IsOverload() {
		t.Fatalf("full limiter: %+v", r)
	}
	a.Done(OK, false)
	b.Done(Overload, true)
	if l.Inflight() != 0 || s.Admitted.Load() != 2 || s.ShedLimit.Load() != 1 {
		t.Fatalf("inflight %d admitted %d shed %d", l.Inflight(), s.Admitted.Load(), s.ShedLimit.Load())
	}
}

// Tier shares: with shares 1/0.5 and limit 4, sheddable traffic stops at 2
// in flight while critical traffic still gets in.
func TestAdmitTierShares(t *testing.T) {
	s := NewServer(ServerConfig{Limiter: limit.NewLimiter(&limit.Fixed{N: 4}, limit.Options{Shares: []float64{1, 0.5}})})
	var held []*Ticket
	for i := 0; i < 2; i++ {
		tk, r := admit(s, context.Background(), priority.Sheddable)
		if r != nil {
			t.Fatal(r)
		}
		held = append(held, tk)
	}
	if _, r := admit(s, context.Background(), priority.Sheddable); r == nil {
		t.Fatal("sheddable admitted past its share")
	}
	for i := 0; i < 2; i++ {
		tk, r := admit(s, context.Background(), priority.Critical)
		if r != nil {
			t.Fatalf("critical refused at %d in flight", 2+i)
		}
		held = append(held, tk)
	}
	for _, tk := range held {
		tk.Done(OK, false)
	}
}

func TestAdmitRateLimitAndDagor(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	rl := ratelimit.New(1, 1, func() time.Time { return now })
	s := NewServer(ServerConfig{RateLimit: rl})
	tk, _ := admit(s, context.Background(), 1)
	tk.Done(OK, false)
	if _, r := admit(s, context.Background(), 1); r == nil || r.Reason != "ratelimit" {
		t.Fatalf("rate limit: %v", r)
	}

	d := priority.NewDagor(func() time.Time { return now })
	s = NewServer(ServerConfig{Dagor: d})
	defer s.Close()
	for d.LevelKey() >= priority.UserLevels { // shed every non-critical key
		d.ObserveDelay(time.Second)
		now = now.Add(d.Window)
		d.Tick()
	}
	if _, r := admit(s, context.Background(), priority.Default); r == nil || r.Reason != "dagor" || s.ShedDagor.Load() != 1 {
		t.Fatalf("dagor: %v", r)
	}
}

func TestAdmitDefaultTimeout(t *testing.T) {
	s := NewServer(ServerConfig{DefaultTimeout: time.Second})
	ctx, tk, _ := s.Admit(context.Background(), priority.Info{Tier: priority.Critical, User: 3})
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("no deadline added")
	}
	if p, ok := priority.FromContext(ctx); !ok || p.Tier != priority.Critical || p.User != 3 {
		t.Fatalf("priority not on context: %v", p)
	}
	tk.Done(OK, false)
	if ctx.Err() == nil {
		t.Fatal("Done did not cancel the default timeout")
	}
}

// Overload outcomes teach the limiter; Ignore does not.
func TestDoneOutcomes(t *testing.T) {
	alg := limit.NewAIMD(50)
	alg.MinLimit = 1
	s := NewServer(ServerConfig{Limiter: limit.NewLimiter(alg, limit.Options{})})
	tk, _ := admit(s, context.Background(), 1)
	tk.Done(Ignore, true)
	if alg.Limit() != 50 {
		t.Fatalf("Ignore moved the limit to %d", alg.Limit())
	}
	tk, _ = admit(s, context.Background(), 1)
	tk.Done(Overload, true)
	if alg.Limit() != 45 {
		t.Fatalf("Overload: limit %d, want 45", alg.Limit())
	}
}

func TestEnvConfig(t *testing.T) {
	vars := map[string]string{
		"LC_LIMIT": "gradient2", "LC_TIER_SHARES": "1, 0.9,0.7", "LC_DAGOR": "wait", "LC_DEADLINE": "on",
		"LC_PUSHBACK_MS": "-1", "LC_ONE_LAYER": "on", "LC_RETRY": "budget", "LC_THROTTLE": "2",
		"LC_FRONT_END_LIMIT": "fixed:7",
	}
	e := Env{Service: "front-end", Lookup: func(k string) (string, bool) { v, ok := vars[k]; return v, ok }}
	sc, err := e.ServerConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Limiter.Algorithm().Limit() != 7 || sc.Dagor == nil || !sc.DropExpired || sc.Pushback != -time.Millisecond || !sc.OneLayer {
		t.Fatalf("server config: %+v", sc)
	}
	cc, err := e.ClientConfig("back", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cc.Throttle == nil || cc.MaxAttempts != 3 || !cc.HonorPushback || !cc.HonorNoRetry {
		t.Fatalf("client config: %+v", cc)
	}
	if _, ok := cc.Budget.(interface{ Tokens() float64 }); !ok {
		t.Fatalf("budget %T", cc.Budget)
	}
	vars["LC_FRONT_END_LIMIT"] = "bogus"
	if _, err := e.ServerConfig(nil); err == nil {
		t.Fatal("bad LIMIT accepted")
	}
}

// BenchmarkServerAdmit is the Admit+Done path alone: bare (nothing
// enabled) against the full server stack used in the overhead experiment.
func BenchmarkServerAdmit(b *testing.B) {
	full := func() ServerConfig {
		return ServerConfig{
			Limiter:     limit.NewLimiter(limit.NewGradient2(1000), limit.Options{Shares: []float64{1, 0.9, 0.7}, MaxWait: 10 * time.Millisecond}),
			Dagor:       priority.NewDagor(nil),
			DagorSignal: "wait", DropExpired: true, MinBudget: time.Millisecond, OneLayer: true,
		}
	}
	for _, c := range []struct {
		name string
		cfg  ServerConfig
	}{{"bare", ServerConfig{}}, {"full", full()}} {
		b.Run(c.name, func(b *testing.B) {
			s := NewServer(c.cfg)
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			info := priority.Info{Tier: priority.Default, User: 1}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, tk, rej := s.Admit(ctx, info)
				if rej != nil {
					b.Fatal(rej)
				}
				tk.Done(OK, false)
			}
		})
	}
}

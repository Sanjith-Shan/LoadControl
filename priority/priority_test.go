package priority

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestParse(t *testing.T) {
	for in, want := range map[string]Tier{
		"critical": Critical, "0": Critical, "default": Default, "": Default, "1": Default,
		"sheddable": Sheddable, "2": Sheddable, "5": 5, "-1": Default, "junk": Default, "64": Default,
	} {
		if got := Parse(in); got != want {
			t.Errorf("Parse(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestContext(t *testing.T) {
	if p, ok := FromContext(context.Background()); ok || p.Tier != Default {
		t.Fatalf("empty context: %v %v", p, ok)
	}
	in := Info{Tier: Critical, User: 7}
	if p, ok := FromContext(WithInfo(context.Background(), in)); !ok || p != in {
		t.Fatalf("round trip: %v %v", p, ok)
	}
}

func TestKeyAndUserPriority(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := Info{Tier: Tier(rapid.IntRange(-5, 10).Draw(t, "tier")), User: rapid.IntRange(-5, 200).Draw(t, "user")}
		if k := Key(p); k < 0 || k > maxKey {
			t.Fatalf("Key(%v) = %d outside [0,%d]", p, k, maxKey)
		}
		u := rapid.String().Draw(t, "user id")
		e := rapid.Uint64().Draw(t, "epoch")
		if v := UserPriority(u, e); v < 0 || v >= UserLevels || v != UserPriority(u, e) {
			t.Fatalf("UserPriority(%q) = %d", u, v)
		}
	})
	if Key(Info{Tier: Critical, User: 5}) >= Key(Info{Tier: Default, User: 0}) {
		t.Fatal("a critical key must sort before every default key")
	}
}

// window feeds one window of arrivals spread over every key, with the
// given mean queueing delay, then closes it.
func window(d *Dagor, c *clock, delay time.Duration) {
	for k := 0; k <= maxKey; k += 3 {
		d.Admit(Info{Tier: Tier(k / UserLevels), User: k % UserLevels})
	}
	d.ObserveDelay(delay)
	c.t = c.t.Add(d.Window)
	d.Tick()
}

// Property (h): under sustained delay the level falls monotonically to key
// 0 and stays there; once delay drops it recovers to admit everything.
func TestDagorLevel(t *testing.T) {
	c := &clock{time.Unix(1_700_000_000, 0)}
	d := NewDagor(c.now)
	prev := d.LevelKey()
	for i := 0; i < 500; i++ {
		window(d, c, 50*time.Millisecond)
		lvl := d.LevelKey()
		if lvl < 0 || lvl > prev || (prev > 0 && lvl == prev) {
			t.Fatalf("window %d: level %d -> %d under overload", i, prev, lvl)
		}
		prev = lvl
		if lvl == 0 {
			break
		}
	}
	if prev != 0 {
		t.Fatalf("level stuck at %d", prev)
	}
	for i := 0; i < 5; i++ {
		window(d, c, 50*time.Millisecond)
		if d.LevelKey() != 0 {
			t.Fatalf("level left 0 while still overloaded: %d", d.LevelKey())
		}
	}
	if !d.Admit(Info{Tier: Critical, User: 0}) {
		t.Fatal("key 0 shed")
	}
	for i := 0; i < 1000 && d.LevelKey() < maxKey; i++ {
		lvl := d.LevelKey()
		window(d, c, time.Millisecond)
		if d.LevelKey() < lvl {
			t.Fatalf("level fell %d -> %d without overload", lvl, d.LevelKey())
		}
	}
	if d.LevelKey() != maxKey {
		t.Fatalf("level %d did not recover to %d", d.LevelKey(), maxKey)
	}
}

// Random arrivals, delays and clock steps: the level stays in range, moves
// in the direction the closing window calls for, and Admit agrees with it.
func TestDagorProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c := &clock{time.Unix(1_700_000_000, 0)}
		d := NewDagor(c.now)
		d.WindowRequests = int64(rapid.IntRange(1, 50).Draw(t, "windowRequests"))
		for i := rapid.IntRange(1, 300).Draw(t, "n"); i > 0; i-- {
			lvl := d.level
			due := c.t.Sub(d.windowStart) >= d.Window || d.arrivals >= d.WindowRequests
			over := d.delayN > 0 && d.delaySum/time.Duration(d.delayN) > d.Threshold
			switch rapid.IntRange(0, 3).Draw(t, "op") {
			case 0:
				p := Info{Tier: Tier(rapid.IntRange(0, NumTiers-1).Draw(t, "tier")), User: rapid.IntRange(0, UserLevels-1).Draw(t, "user")}
				if got := d.Admit(p); got != (Key(p) <= d.LevelKey()) {
					t.Fatalf("Admit(key %d) = %v at level %d", Key(p), got, d.LevelKey())
				}
			case 1:
				d.ObserveDelay(time.Duration(rapid.Int64Range(0, int64(100*time.Millisecond)).Draw(t, "delay")))
				continue
			case 2:
				c.t = c.t.Add(time.Duration(rapid.Int64Range(0, int64(2*time.Second)).Draw(t, "advance")))
				continue
			case 3:
				d.Tick()
			}
			now := d.LevelKey()
			switch {
			case now < 0 || now > maxKey:
				t.Fatalf("level %d out of range", now)
			case !due && now != lvl:
				t.Fatalf("level moved %d -> %d mid-window", lvl, now)
			case due && over && lvl > 0 && now >= lvl:
				t.Fatalf("overloaded window: level %d -> %d", lvl, now)
			case due && !over && now < lvl:
				t.Fatalf("healthy window: level %d -> %d", lvl, now)
			}
		}
	})
}

func TestSchedLatency(t *testing.T) {
	s := NewSchedLatency()
	var wg sync.WaitGroup
	for i := 0; i < 4*runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
	m, n := s.Mean()
	if n == 0 || m < 0 || m > time.Second {
		t.Fatalf("Mean() = %v over %d events", m, n)
	}
	if _, n := s.Mean(); n > 1000 {
		t.Fatalf("second Mean() counted %d events, want only new ones", n)
	}
}

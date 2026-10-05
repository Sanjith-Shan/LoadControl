package priority

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Dagor is the overload detector and admission level from DAGOR (Zhou et
// al., SoCC 2018). Every request has a compound priority key
// (tier, user priority); the server admits a request only if its key is at
// or below the admission level. Once per window the level moves:
//
//   - if the average queueing delay in the window exceeded Threshold, the
//     level drops until about Alpha (5%) fewer of the window's requests
//     would have been admitted;
//   - otherwise it rises until about Beta (1%) more would have been.
//
// The histogram of the window's arrivals by key is what lets it move by a
// fraction of load instead of a whole tier at a time.
type Dagor struct {
	Threshold      time.Duration // overload when mean queueing delay exceeds this (20 ms in the paper)
	Alpha, Beta    float64       // shed step and admit step
	Window         time.Duration // adjust at least this often
	WindowRequests int64         // or after this many arrivals
	now            func() time.Time

	mu          sync.Mutex
	level       int // highest admitted key
	hist        []int64
	arrivals    int64
	delaySum    time.Duration
	delayN      int64
	windowStart time.Time
	// Overloaded windows seen, for metrics and tests.
	overloadedWindows int64
}

const maxKey = NumTiers*UserLevels - 1

// Key is the compound priority used for admission. Lower is more
// important.
func Key(p Info) int {
	t := int(p.Tier)
	if t < 0 {
		t = 0
	}
	if t >= NumTiers {
		t = NumTiers - 1
	}
	u := p.User
	if u < 0 || u >= UserLevels {
		u = UserLevels - 1
	}
	return t*UserLevels + u
}

// NewDagor returns a controller with the paper's constants: 20 ms
// threshold, alpha 5%, beta 1%, window 1 s or 2000 requests.
func NewDagor(now func() time.Time) *Dagor {
	if now == nil {
		now = time.Now
	}
	return &Dagor{
		Threshold: 20 * time.Millisecond, Alpha: 0.05, Beta: 0.01,
		Window: time.Second, WindowRequests: 2000, now: now,
		level: maxKey, hist: make([]int64, maxKey+1), windowStart: now(),
	}
}

// Admit records the arrival and reports whether p is at or below the
// current admission level.
func (d *Dagor) Admit(p Info) bool {
	k := Key(p)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maybeAdjust()
	d.hist[k]++
	d.arrivals++
	return k <= d.level
}

// ObserveDelay feeds one queueing-delay sample (time a request waited
// before a worker started on it).
func (d *Dagor) ObserveDelay(q time.Duration) {
	d.mu.Lock()
	d.delaySum += q
	d.delayN++
	d.mu.Unlock()
}

// Level returns the current admission level as (tier, user priority).
func (d *Dagor) Level() (Tier, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Tier(d.level / UserLevels), d.level % UserLevels
}

// LevelKey returns the admission level as a compound key.
func (d *Dagor) LevelKey() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.level
}

// OverloadedWindows returns how many windows were judged overloaded.
func (d *Dagor) OverloadedWindows() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.overloadedWindows
}

// Tick forces a window check; servers with little traffic call it from a
// ticker so the level can recover while idle.
func (d *Dagor) Tick() {
	d.mu.Lock()
	d.maybeAdjust()
	d.mu.Unlock()
}

func (d *Dagor) maybeAdjust() {
	now := d.now()
	if now.Sub(d.windowStart) < d.Window && d.arrivals < d.WindowRequests {
		return
	}
	overloaded := d.delayN > 0 && d.delaySum/time.Duration(d.delayN) > d.Threshold
	var admitted int64
	for k := 0; k <= d.level; k++ {
		admitted += d.hist[k]
	}
	if overloaded {
		d.overloadedWindows++
		target := int64(float64(admitted) * (1 - d.Alpha))
		var cum int64
		lvl := -1
		for k := 0; k <= d.level; k++ {
			if cum+d.hist[k] > target {
				break
			}
			cum += d.hist[k]
			lvl = k
		}
		if lvl == d.level && d.level > 0 {
			lvl = d.level - 1 // always make progress when overloaded
		}
		if lvl < 0 {
			lvl = 0 // never shed the most important key entirely
		}
		d.level = lvl
	} else if d.level < maxKey {
		target := int64(float64(admitted)*(1+d.Beta)) + 1
		cum := admitted
		lvl := d.level
		for k := d.level + 1; k <= maxKey; k++ {
			cum += d.hist[k]
			lvl = k
			if cum >= target {
				break
			}
		}
		d.level = lvl
	}
	for i := range d.hist {
		d.hist[i] = 0
	}
	d.arrivals, d.delaySum, d.delayN = 0, 0, 0
	d.windowStart = now
}

// RandomUser returns a user priority for requests that carry no identity.
func RandomUser() int { return rand.IntN(UserLevels) }

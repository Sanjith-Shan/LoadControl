package sim

import (
	"flag"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"
)

// Record is one measured run as bench/lcbench.py writes it (the fields
// replay needs).
type Record struct {
	Exp        string            `json:"exp"`
	Name       string            `json:"name"`
	Time       string            `json:"time"`
	Env        map[string]string `json:"env"`
	LoadgenCmd string            `json:"loadgen_cmd"`
	Rate       float64           `json:"rate"`
	Schedule   string            `json:"schedule"`
	DurationS  float64           `json:"duration_s"`
	Faults     []Fault           `json:"faults"`
	Summary    struct {
		Offered int64    `json:"offered"`
		Good    int64    `json:"good"`
		Slow    int64    `json:"slow"`
		Shed    int64    `json:"shed"`
		Error   int64    `json:"error"`
		Timeout int64    `json:"timeout"`
		P50     *float64 `json:"p50_ms"`
		P99     *float64 `json:"p99_ms"`
	} `json:"summary"`
	Series []struct {
		T    int   `json:"t"`
		Good int64 `json:"good"`
	} `json:"series"`
	Note     string          `json:"note"`
	Restarts map[string]*int `json:"restarts"`
	Load     struct {
		Before, Mid, After *hostLoad
		HostCPU            []float64 `json:"host_cpu_pct_per_2s"`
	} `json:"load"`
}

type hostLoad struct {
	WindowsCPU *float64 `json:"windows_cpu_pct"`
	PeerLock   *string  `json:"peer_lock"`
}

// Dirty mirrors bench/lcnumbers.py clean(): a run is contaminated if a peer
// lock was seen, a service restarted, the host was saturated just before or
// after it (Windows CPU >= 90%), or something else took a core during it
// (mid-run sample or median of the continuous samples >= 72%; the VM alone
// shows 55-65%). It returns the reason, or "" for a clean run.
func (r *Record) Dirty() string {
	L := map[string]*hostLoad{"before": r.Load.Before, "mid": r.Load.Mid, "after": r.Load.After}
	for _, k := range []string{"before", "mid", "after"} {
		if l := L[k]; l != nil && l.PeerLock != nil && *l.PeerLock != "" {
			return "peer lock " + k
		}
	}
	for _, s := range sortedKeys(r.Restarts) {
		if n := r.Restarts[s]; n != nil && *n > 0 {
			return s + " restarted"
		}
	}
	for _, k := range []string{"before", "after"} {
		if l := L[k]; l != nil && l.WindowsCPU != nil && *l.WindowsCPU >= 90 {
			return fmt.Sprintf("host CPU %.0f%% %s", *l.WindowsCPU, k)
		}
	}
	if l := r.Load.Mid; l != nil && l.WindowsCPU != nil && *l.WindowsCPU >= 72 {
		return fmt.Sprintf("host CPU %.0f%% mid-run", *l.WindowsCPU)
	}
	if hs := r.Load.HostCPU; len(hs) > 0 {
		c := append([]float64(nil), hs...)
		sort.Float64s(c)
		if c[len(c)/2] >= 72 {
			return fmt.Sprintf("host CPU median %.0f%% during run", c[len(c)/2])
		}
	}
	return ""
}

// ReplayParams builds the simulation of a measured run from base: the
// run's LC_* env, rate or schedule, duration, fault timeline and the
// load generator flags parsed from its command line. lcbench's warm-up
// (15 s at 100 req/s with the same config) is simulated and not reported.
func ReplayParams(base *Params, r *Record) (*Params, error) {
	p := base.Clone()
	p.Env = map[string]string{}
	for k, v := range r.Env {
		p.Env[k] = v
	}
	p.DurationS, p.WarmupS = r.DurationS, 0
	p.Flush, p.Slow = Trigger{}, Trigger{}
	p.Faults = r.Faults
	p.Load = LoadParams{Process: "constant", RPS: r.Rate, Schedule: r.Schedule, WarmupS: 15, WarmupRPS: 100}

	// cmd/loadgen's flags, with its defaults.
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.String("target", "", "")
	fs.String("out", "", "")
	fs.String("label", "", "")
	fs.Int("conns", 0, "")
	fs.Float64("rate", 100, "")
	fs.Duration("duration", time.Minute, "")
	schedule := fs.String("schedule", "", "")
	timeout := fs.Duration("timeout", time.Second, "")
	retries := fs.Int("retries", 0, "")
	backoff := fs.Duration("backoff", 0, "")
	noRetry := fs.Bool("honor-no-retry", false, "")
	retryAf := fs.Bool("honor-retry-after", false, "")
	retryShed := fs.Bool("retry-shed", true, "")
	slo := fs.Duration("slo", 500*time.Millisecond, "")
	tiers := fs.String("tiers", "0.2,0.3,0.5", "")
	users := fs.Int("users", 1000, "")
	poisson := fs.Bool("poisson", false, "")
	deadline := fs.Bool("deadline-header", false, "")
	seed := fs.Uint64("seed", 1, "")
	args := strings.Fields(r.LoadgenCmd)
	if len(args) > 0 {
		args = args[1:] // the binary
	}
	for i, a := range args { // shell quoting from lcbench (shlex.quote)
		args[i] = strings.Trim(a, "'")
	}
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("loadgen_cmd %q: %v", r.LoadgenCmd, err)
	}
	if *schedule != "" {
		p.Load.Schedule = *schedule
	}
	if *poisson {
		p.Load.Process = "poisson"
	}
	ts, err := parseFloats(*tiers)
	if err != nil {
		return nil, err
	}
	p.Tiers = ts
	p.Seed = *seed
	p.SLOMS = float64(*slo) / 1e6
	p.User = UserParams{Users: *users, TimeoutMS: float64(*timeout) / 1e6, Retries: *retries,
		BackoffMS: float64(*backoff) / 1e6, HonorNoRetry: *noRetry, HonorPushback: *retryAf,
		SendDeadline: *deadline, RetryShed: *retryShed}
	return p, nil
}

// RecoveryS is bench/lcnumbers.py's recovery(): seconds after the trigger is
// removed (off) until goodput per completion second stays at >= frac of
// its mean over [baseFrom, on) for hold seconds; nil if it never does.
func RecoveryS(good map[int]int64, duration int) *int {
	const baseFrom, on, off, frac, hold = 10, 30, 50, 0.9, 10
	var base float64
	for t := baseFrom; t < on; t++ {
		base += float64(good[t])
	}
	base /= on - baseFrom
	for t := off; t <= duration-hold; t++ {
		ok := true
		for u := t; u < t+hold && ok; u++ {
			ok = float64(good[u]) >= frac*base
		}
		if ok {
			r := t - off
			return &r
		}
	}
	return nil
}

// Comparison is one replayed run: measured against simulated.
type Comparison struct {
	Exp          string   `json:"exp"`
	Name         string   `json:"name"`
	File         string   `json:"file"`
	Time         string   `json:"time"`
	Faults       int      `json:"faults"`
	Contaminated string   `json:"contaminated,omitempty"` // why the run is excluded from error stats (bench/lcnumbers.py clean())
	Unsupported  []string `json:"unsupported,omitempty"`
	Measured     Outcome  `json:"measured"`
	Sim          Outcome  `json:"sim"`
	AbsErr       float64  `json:"abs_err_good_rps"`       // sim - measured
	RelErr       *float64 `json:"rel_err_good,omitempty"` // |sim - measured| / measured, if measured >= 5 req/s
	WallMS       float64  `json:"wall_ms"`
}

// Outcome is a run's user-visible result, per second over the run.
type Outcome struct {
	OfferedRPS float64 `json:"offered_rps"`
	GoodRPS    float64 `json:"good_rps"`
	SlowRPS    float64 `json:"slow_rps"`
	ShedRPS    float64 `json:"shed_rps"`
	ErrorRPS   float64 `json:"error_rps"`
	TimeoutRPS float64 `json:"timeout_rps"`
	RecoveryS  *int    `json:"recovery_s,omitempty"`
}

// Replay simulates one record with base params and compares.
func Replay(base *Params, r *Record, file string) (*Comparison, *Result, error) {
	p, err := ReplayParams(base, r)
	if err != nil {
		return nil, nil, err
	}
	s, err := New(p)
	if err != nil {
		return nil, nil, err
	}
	res := s.Run()
	d := r.DurationS
	c := &Comparison{Exp: r.Exp, Name: r.Name, File: file, Time: r.Time, Faults: len(r.Faults), Contaminated: r.Dirty(),
		Unsupported: s.Unsupported, WallMS: res.Summary.WallMS}
	m := r.Summary
	c.Measured = Outcome{OfferedRPS: round(float64(m.Offered) / d), GoodRPS: round(float64(m.Good) / d),
		SlowRPS: round(float64(m.Slow) / d), ShedRPS: round(float64(m.Shed) / d),
		ErrorRPS: round(float64(m.Error) / d), TimeoutRPS: round(float64(m.Timeout) / d)}
	t := res.Summary.Totals
	c.Sim = Outcome{OfferedRPS: round(float64(t.Offered) / d), GoodRPS: round(float64(t.Good) / d),
		SlowRPS: round(float64(t.Slow) / d), ShedRPS: round(float64(t.Shed) / d),
		ErrorRPS: round(float64(t.Error) / d), TimeoutRPS: round(float64(t.Timeout) / d)}
	if len(r.Faults) > 0 {
		mg, sg := map[int]int64{}, map[int]int64{}
		for _, x := range r.Series {
			mg[x.T] = x.Good
		}
		for _, x := range res.Seconds {
			sg[x.T] = x.Goodput
		}
		c.Measured.RecoveryS = RecoveryS(mg, int(d))
		c.Sim.RecoveryS = RecoveryS(sg, int(d))
	}
	c.AbsErr = round(c.Sim.GoodRPS - c.Measured.GoodRPS)
	if c.Measured.GoodRPS >= 5 { // relative error is meaningless near zero
		e := round(math.Abs(c.AbsErr) / c.Measured.GoodRPS)
		c.RelErr = &e
	}
	return c, res, nil
}

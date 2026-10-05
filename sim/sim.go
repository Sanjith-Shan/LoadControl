// Package sim is a deterministic discrete-event simulator of the
// DeathStarBench hotelReservation topology on one small VM, driving
// LoadControl's own policy code on a virtual clock.
//
// What is the library's code and what is re-expressed: the limit
// algorithms (limit.Algorithm), DAGOR (priority.Dagor), the adaptive
// throttle (throttle.Throttle), the retry budgets (retry.Budget,
// retry.RatioBudget, retry.Unlimited), the token bucket
// (ratelimit.Bucket), pushback parsing (retry.Pushback) and the whole
// configuration surface (loadcontrol.Env) are called directly, with
// clocks injected. What blocks goroutines in the real library (the
// limiter's wait queue, the client's attempt loop, timers) is
// re-expressed as events here with the same decisions in the same order;
// see limiter.go and model.go.
package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/ratelimit"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"github.com/Sanjith-Shan/LoadControl/throttle"
)

// Sim is one run. Not safe for concurrent use; run several Sims in
// parallel instead.
type Sim struct {
	P *Params

	now    int64
	seq    uint64
	q      eventQ
	ready  []func()
	rh     int
	cpu    cpu
	events int64

	rArr, rSvc, rKey, rMisc *rand.Rand

	svc      [nSvc]*service
	entry    [nReq]*handler // frontend handlers per request type
	caches   []*cache
	mix      [nReq]float64 // cumulative
	tiers    []float64     // cumulative
	userPrio []int
	steps    []step
	busyAtT0 float64

	secs      []*sec
	scratch   sec   // stats from the warm-up phase, discarded
	t0        int64 // end of the warm-up phase; reported time starts here
	deps      map[string]*dep
	mongoProc map[string]*proc
	host      *proc
	// Unsupported lists fault specs the model ignored.
	Unsupported []string
	outstanding int
	endNs       int64
	lastBusy    float64
	typLat      [nReq][]float64
	typN, typOK [nReq]int64
}

// New builds a run from p (which it does not modify).
func New(p *Params) (*Sim, error) {
	p = p.Clone()
	if p.Cores <= 0 {
		p.Cores = 1
	}
	t0 := ms(p.Load.WarmupS * 1e3)
	s := &Sim{P: p, t0: t0, endNs: t0 + ms(p.DurationS*1e3), deps: map[string]*dep{}}
	s.cpu.cores, s.cpu.fcfs, s.cpu.procs = p.Cores, p.CPU == "fcfs", p.CPU == "procs"
	s.cpu.slice = p.SliceMS * 1e6
	s.cpu.runnext = !p.NoRunnext
	if s.cpu.procs && s.cpu.slice == 0 {
		s.cpu.slice = 10e6
	}
	s.rArr = rand.New(rand.NewPCG(p.Seed, 1))
	s.rSvc = rand.New(rand.NewPCG(p.Seed, 2))
	s.rKey = rand.New(rand.NewPCG(p.Seed, 3))
	s.rMisc = rand.New(rand.NewPCG(p.Seed, 4))
	var err error
	if s.steps, err = p.steps(); err != nil {
		return nil, err
	}
	if t0 > 0 {
		for i := range s.steps {
			s.steps[i].t += p.Load.WarmupS
		}
		s.steps = append([]step{{0, p.Load.WarmupRPS}}, s.steps...)
	}
	var tot float64
	for i, n := range reqNames {
		tot += p.Mix[n]
		s.mix[i] = tot
	}
	if tot <= 0 {
		return nil, fmt.Errorf("mix is empty")
	}
	for i := range s.mix {
		s.mix[i] /= tot
	}
	tot = 0
	for _, t := range p.Tiers {
		tot += t
		s.tiers = append(s.tiers, tot)
	}
	for i := range s.tiers {
		s.tiers[i] /= tot
	}
	// cmd/loadgen sends the user index as X-Lc-User; lchttp takes 0..127
	// as the priority itself and hashes anything else.
	s.userPrio = make([]int, max(1, p.User.Users))
	for i := range s.userPrio {
		s.userPrio[i] = i
		if i >= priority.UserLevels {
			s.userPrio[i] = priority.UserPriority(strconv.Itoa(i), 0)
		}
	}
	if err := s.build(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Sim) build() error {
	p := s.P
	lookup := func(k string) (string, bool) { v, ok := p.Env[k]; return v, ok }
	s.mongoProc = map[string]*proc{}
	s.host = s.newProc("host", "host", true)
	for i, name := range svcNames {
		v := &service{name: name, id: i, http: i == svcFrontend, maxConc: p.MaxConcurrency[name],
			dbTODO: p.Mongo.CtxTODO[name], pool: p.Mongo.Pool}
		env := lc.Env{Service: name, Lookup: lookup}
		cfg, err := env.ServerConfig(nil)
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
		v.cfg = cfg
		get := func(key, def string) string { return envGet(lookup, name, key, def) }
		// Rebuild the clocked pieces on the virtual clock, keeping the
		// values the library parsed.
		if cfg.Limiter != nil {
			alg := cfg.Limiter.Algorithm()
			if vg, ok := alg.(*limit.Vegas); ok {
				seedRand(vg, s.rMisc.Float64)
			}
			v.lim = &limiter{alg: alg}
			if v.lim.shares, err = parseFloats(get("TIER_SHARES", "")); err != nil {
				return err
			}
			w, _ := strconv.ParseFloat(get("QUEUE_WAIT_MS", "0"), 64)
			v.lim.maxWait = ms(w)
		}
		if cfg.Dagor != nil {
			v.dagor = priority.NewDagor(s.clock)
			v.dagor.Threshold = cfg.Dagor.Threshold
			v.sched = cfg.DagorSignal == "sched"
		}
		if cfg.RateLimit != nil {
			v.rl = ratelimit.New(cfg.RateLimit.Rate, cfg.RateLimit.Burst, s.clock)
		}
		v.proc = s.newProc(name, "go", true)
		if name == "rate" || name == "profile" || name == "reservation" {
			s.mongoProc[name] = s.newProc("mongo-"+name, "mongod", false)
		}
		s.svc[i] = v
	}
	h := func(svc int, method string, steps ...hstep) *handler {
		name := svcNames[svc] + "." + method
		x := &handler{name: name, svc: s.svc[svc], cpu: p.Handlers[name], steps: append([]hstep{{kind: stCPU}}, steps...)}
		return x
	}
	call := func(x *handler) hstep { return hstep{kind: stCall, to: x} }
	mk := func(name string) *cache {
		cp := p.Caches[name]
		c := &cache{name: name, id: len(s.caches), present: make([]bool, max(1, cp.Keys)), perReq: max(1, cp.PerReq), proc: s.newProc("memc-"+name, "memcached", false)}
		for i := range c.present {
			c.present[i] = true // the real system runs warm
		}
		s.caches = append(s.caches, c)
		return c
	}
	cRate, cProfile, cReserve := mk("rate"), mk("profile"), mk("reserve")
	geo := h(svcGeo, "nearby")
	rate := h(svcRate, "get", hstep{kind: stCache, cache: cRate})
	search := h(svcSearch, "nearby", call(geo), call(rate))
	check := h(svcReservation, "check", hstep{kind: stCache, cache: cReserve})
	makeRes := h(svcReservation, "make", hstep{kind: stCache, cache: cReserve}, hstep{kind: stDB})
	profile := h(svcProfile, "get", hstep{kind: stCache, cache: cProfile})
	rec := h(svcRecommendation, "get")
	user := h(svcUser, "check")
	s.entry[reqSearch] = h(svcFrontend, "search", call(search), call(check), call(profile))
	s.entry[reqRecommend] = h(svcFrontend, "recommend", call(rec), call(profile))
	s.entry[reqUser] = h(svcFrontend, "user", call(user))
	s.entry[reqReserve] = h(svcFrontend, "reserve", call(user), call(makeRes))

	edges := [][2]int{{svcFrontend, svcSearch}, {svcFrontend, svcReservation}, {svcFrontend, svcProfile},
		{svcFrontend, svcRecommendation}, {svcFrontend, svcUser}, {svcSearch, svcGeo}, {svcSearch, svcRate}}
	for i := range s.svc {
		s.svc[i].leaf = true
	}
	for _, ed := range edges {
		from := s.svc[ed[0]]
		from.leaf = false
		env := lc.Env{Service: from.name, Lookup: lookup}
		cfg, err := env.ClientConfig(svcNames[ed[1]], nil)
		if err != nil {
			return fmt.Errorf("%s: %v", from.name, err)
		}
		if cfg.Throttle != nil {
			win, _ := strconv.ParseFloat(envGet(lookup, from.name, "THROTTLE_WINDOW_S", "120"), 64)
			cfg.Throttle = throttle.New(cfg.Throttle.K, time.Duration(win)*time.Second, s.clock)
		}
		if rb, ok := cfg.Budget.(*retry.RatioBudget); ok {
			cfg.Budget = retry.NewRatioBudget(rb.Ratio, rb.MinPerSec, 10*time.Second, s.clock)
		}
		from.client[ed[1]] = &client{cfg: cfg}
	}
	return nil
}

// newProc makes a process for "procs" mode with the slots of its kind.
func (s *Sim) newProc(name, kind string, preempt bool) *proc {
	n := s.P.Slots[kind]
	if n <= 0 {
		n = map[string]int{"go": 2, "memcached": 4, "mongod": 4, "host": 2}[kind]
	}
	return &proc{name: name, slots: n, preempt: preempt}
}

func envGet(lookup func(string) (string, bool), svc, key, def string) string {
	if x, ok := lookup("LC_" + strings.ToUpper(svc) + "_" + key); ok {
		return x
	}
	if x, ok := lookup("LC_" + key); ok && x != "" {
		return x
	}
	return def
}

func parseFloats(s string) ([]float64, error) {
	if s == "" {
		return nil, nil
	}
	var out []float64
	for _, f := range strings.Split(s, ",") {
		x, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil {
			return nil, fmt.Errorf("TIER_SHARES %q: %v", s, err)
		}
		out = append(out, x)
	}
	return out, nil
}

// seedRand points limit.Vegas's probe jitter at the seeded source and
// redraws the first jitter. Vegas keeps its random source in unexported
// fields defaulting to the global generator, which would make Vegas runs
// irreproducible; if the fields ever change this quietly does nothing.
func seedRand(v *limit.Vegas, f func() float64) {
	set := func(name string, x any) {
		fv := reflect.ValueOf(v).Elem().FieldByName(name)
		if fv.IsValid() && fv.Type() == reflect.TypeOf(x) {
			reflect.NewAt(fv.Type(), unsafe.Pointer(fv.UnsafeAddr())).Elem().Set(reflect.ValueOf(x))
		}
	}
	set("rnd", f)
	set("probeJitter", 0.5+f()*0.5)
}

// Run simulates until arrivals stop and every user request has ended.
func (s *Sim) Run() *Result {
	wall := time.Now()
	s.at(0, s.nextArrival)
	s.at(s.t0+1e9, s.tickSecond)
	s.at(s.t0, func() { s.cpu.advance(s.now); s.busyAtT0 = s.cpu.busy })
	for _, v := range s.svc {
		if v.dagor != nil {
			v := v
			var tick func()
			tick = func() {
				if v.sched {
					jobs := float64(s.cpu.load())
					if d := (jobs/s.cpu.cores - 1) * s.P.SchedQuantumMS; d > 0 {
						v.dagor.ObserveDelay(time.Duration(d * 1e6))
					}
				}
				v.dagor.Tick()
				if s.now < s.endNs {
					s.at(s.now+1e8, tick)
				}
			}
			s.at(1e8, tick)
		}
	}
	s.schedFaults()
	for (s.now < s.endNs || s.outstanding > 0) && s.step() {
	}
	return s.result(time.Since(wall))
}

// Run is New followed by Run.
func Run(p *Params) (*Result, error) {
	s, err := New(p)
	if err != nil {
		return nil, err
	}
	return s.Run(), nil
}

// rateAt is the offered rate at time t and when it next changes.
func (s *Sim) rateAt(t int64) (float64, int64) {
	r, next := 0.0, int64(-1)
	for _, st := range s.steps {
		at := ms(st.t * 1e3)
		if at <= t {
			r = st.x
		} else {
			next = at
			break
		}
	}
	return r, next
}

// nextArrival schedules the next user request (open loop).
func (s *Sim) nextArrival() {
	r, change := s.rateAt(s.now)
	if r <= 0 {
		if change > 0 && change < s.endNs {
			s.at(change, s.nextArrival)
		}
		return
	}
	gap := 1e9 / r
	if s.P.Load.Process != "constant" {
		gap *= s.rArr.ExpFloat64()
	}
	t := s.now + int64(gap)
	if change > 0 && t >= change { // memoryless: resample at the rate change
		s.at(change, s.nextArrival)
		return
	}
	if t >= s.endNs {
		return
	}
	s.at(t, func() { s.arrive(); s.nextArrival() })
}

type ureq struct {
	typ      int
	in       info
	start    int64
	attempts int
	warm     bool // sent during the warm-up phase, not counted
}

func (s *Sim) arrive() {
	u := &ureq{start: s.now, warm: s.now < s.t0}
	x := s.rArr.Float64()
	for u.typ < nReq-1 && x >= s.mix[u.typ] {
		u.typ++
	}
	x = s.rArr.Float64()
	t := 0
	for t < len(s.tiers)-1 && x >= s.tiers[t] {
		t++
	}
	u.in = info{Tier: priority.Tier(t), User: s.userPrio[s.rArr.IntN(len(s.userPrio))]}
	st := s.sec()
	st.offered++
	st.tier[min(t, 2)].offered++
	s.outstanding++
	s.userAttempt(u)
}

// userAttempt is one HTTP request from the load generator with its own
// timeout; on timeout the connection closes, which cancels the frontend's
// request context.
func (s *Sim) userAttempt(u *ureq) {
	u.attempts++
	s.sec().user[min(u.attempts-1, 1)]++
	a := s.newCtx(nil, s.now+ms(s.P.User.TimeoutMS), true)
	settled := false
	settle := func(r reply) {
		if settled {
			return
		}
		settled = true
		s.cancel(a, Canceled)
		s.post(func() { s.userResult(u, r) })
	}
	a.onDone(func() { settle(reply{code: DeadlineExceeded, local: true}) })
	s.at(s.now+ms(s.P.NetMS), func() { s.serve(s.entry[u.typ], a, u.in, u.attempts, settle) })
}

// User outcomes, as cmd/loadgen classifies the final attempt.
const (
	oGood    = iota // 2xx within the SLO
	oSlow           // 2xx over the SLO
	oShed           // 503 with X-Lc-Shed
	oError          // any other 5xx
	oTimeout        // the client timed out
	nOut
)

// userResult follows cmd/loadgen's retry rules: timeouts and 5xx are
// retried up to user.retries times; a shed 503 only if retry_shed; with
// honor_pushback a negative pushback stops retries and a positive one is
// waited out; with honor_no_retry a marked response is not retried; the
// backoff is exponential with full jitter on top of any pushback.
func (s *Sim) userResult(u *ureq, r reply) {
	if r.code == OK {
		if float64(s.now-u.start)/1e6 <= s.P.SLOMS {
			s.endUser(u, oGood)
		} else {
			s.endUser(u, oSlow)
		}
		return
	}
	up := s.P.User
	res, retryable, wait := oError, true, time.Duration(0)
	switch {
	case r.local:
		res = oTimeout
	case r.code == ResourceExhausted && r.shed != "":
		res, retryable = oShed, up.RetryShed
		if up.HonorPushback && r.pushback != "" {
			n, _ := strconv.Atoi(r.pushback)
			if n < 0 {
				retryable = false
			}
			wait = time.Duration(n) * time.Millisecond
		}
	}
	if up.HonorNoRetry && r.noRetry {
		retryable = false
	}
	if !retryable || u.attempts > up.Retries {
		s.endUser(u, res)
		return
	}
	if up.BackoffMS > 0 {
		wait += time.Duration(s.rMisc.Float64() * up.BackoffMS * 1e6 * math.Pow(2, float64(u.attempts-1)))
	}
	s.at(s.now+int64(wait), func() { s.userAttempt(u) })
}

// Dependencies behind toxiproxy, by proxy name.
type dep struct {
	extra int64 // injected latency per response
	down  bool
}

func (s *Sim) dep(name string) *dep {
	d := s.deps[name]
	if d == nil {
		d = &dep{}
		s.deps[name] = d
	}
	return d
}

// schedFaults schedules the fault timeline (bench/lcbench.py's specs) and
// the flush/slow shorthand triggers.
func (s *Sim) schedFaults() {
	at := func(sec float64, fn func()) { s.at(s.t0+ms(sec*1e3), fn) }
	if f := s.P.Flush; f.At > 0 || f.Dur > 0 {
		at(f.At, func() {
			for _, c := range s.caches {
				if f.hits(c.name) {
					clear(c.present)
					c.disabled = s.t0 + ms((f.At+f.Dur)*1e3)
				}
			}
		})
	}
	if sl := s.P.Slow; sl.ExtraMS > 0 && sl.Dur > 0 {
		for _, name := range []string{"rate", "profile", "reservation"} {
			if sl.hits(name) {
				d := s.dep("mongo-" + name)
				at(sl.At, func() { d.extra += ms(sl.ExtraMS) })
				at(sl.At+sl.Dur, func() { d.extra -= ms(sl.ExtraMS) })
			}
		}
	}
	for _, f := range s.P.Faults {
		at(f.At, func() { s.fault(f.Spec) })
	}
}

// fault applies one spec: flush:<cache>, latency:<proxy>:<ms>[:<jitter>],
// down:<proxy>, up:<proxy>, clear. Others (cpu:) are not modeled.
func (s *Sim) fault(spec string) {
	a := strings.Split(spec, ":")
	switch {
	case a[0] == "flush" && len(a) > 1:
		for _, c := range s.caches {
			if c.name == a[1] {
				clear(c.present)
			}
		}
	case a[0] == "latency" && len(a) > 2:
		v, _ := strconv.ParseFloat(a[2], 64)
		s.dep(a[1]).extra = ms(v)
	case a[0] == "down" && len(a) > 1:
		s.dep(a[1]).down = true
	case a[0] == "up" && len(a) > 1:
		s.dep(a[1]).down = false
	case a[0] == "clear":
		for _, d := range s.deps {
			*d = dep{}
		}
	default:
		s.Unsupported = append(s.Unsupported, spec)
	}
}

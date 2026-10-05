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

	secs        []*sec
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
	s := &Sim{P: p, endNs: int64(p.DurationS * 1e9)}
	s.cpu.cores, s.cpu.fcfs = p.Cores, p.CPU == "fcfs"
	s.rArr = rand.New(rand.NewPCG(p.Seed, 1))
	s.rSvc = rand.New(rand.NewPCG(p.Seed, 2))
	s.rKey = rand.New(rand.NewPCG(p.Seed, 3))
	s.rMisc = rand.New(rand.NewPCG(p.Seed, 4))
	var err error
	if s.steps, err = p.steps(); err != nil {
		return nil, err
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
	// x-lc-user carries a user name; servers hash it with UserPriority.
	s.userPrio = make([]int, max(1, p.User.Users))
	for i := range s.userPrio {
		s.userPrio[i] = priority.UserPriority("user_"+strconv.Itoa(i), 0)
	}
	if err := s.build(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Sim) build() error {
	p := s.P
	lookup := func(k string) (string, bool) { v, ok := p.Env[k]; return v, ok }
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
		c := &cache{name: name, id: len(s.caches), present: make([]bool, max(1, cp.Keys)), perReq: max(1, cp.PerReq)}
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
	s.at(1e9, s.tickSecond)
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
	if f := s.P.Flush; f.At > 0 || f.Dur > 0 {
		s.at(ms(f.At*1e3), func() {
			for _, c := range s.caches {
				if f.hits(c.name) {
					clear(c.present)
					c.disabled = ms((f.At + f.Dur) * 1e3)
				}
			}
		})
	}
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
}

func (s *Sim) arrive() {
	u := &ureq{start: s.now}
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

func (s *Sim) userResult(u *ureq, r reply) {
	if r.code == OK {
		s.endUser(u, OK)
		return
	}
	up := s.P.User
	if u.attempts <= up.Retries && !(up.HonorNoRetry && r.noRetry) {
		wait := s.backoff(retry.Backoff{Initial: time.Duration(ms(up.BackoffMS)), Max: time.Second, Multiplier: 2}, u.attempts)
		ok := true
		if up.HonorPushback {
			d, present, okp := retry.Pushback(r.pushback)
			ok = okp
			if present {
				wait = d
			}
		}
		if ok {
			s.at(s.now+int64(wait), func() { s.userAttempt(u) })
			return
		}
	}
	if r.local {
		s.endUser(u, DeadlineExceeded)
	} else {
		s.endUser(u, Internal)
	}
}

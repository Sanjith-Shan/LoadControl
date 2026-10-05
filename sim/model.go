package sim

import (
	"math"
	"strconv"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/ratelimit"
	"github.com/Sanjith-Shan/LoadControl/retry"
)

// The hotelReservation topology. Every service and both kinds of database
// run on one VM, so all CPU work goes through the single shared cpu.
const (
	svcFrontend = iota
	svcSearch
	svcGeo
	svcRate
	svcReservation
	svcProfile
	svcRecommendation
	svcUser
	nSvc
)

var svcNames = [nSvc]string{"frontend", "search", "geo", "rate", "reservation", "profile", "recommendation", "user"}

// User request types, in mix order.
const (
	reqSearch = iota
	reqRecommend
	reqUser
	reqReserve
	nReq
)

var reqNames = [nReq]string{"search", "recommend", "user", "reserve"}

var tierNames = [priority.NumTiers]string{"critical", "default", "sheddable"}

// Shed reasons as server.go names them.
const (
	shedDeadline = iota
	shedRate
	shedDagor
	shedLimit
	nShed
)

var shedNames = [nShed]string{"deadline", "ratelimit", "dagor", "limit"}

// Client-side local decisions, as client.go's ClientLocal metric.
const (
	localThrottle = iota
	localBudget
	localNoRetry
	nLocal
)

var localNames = [nLocal]string{"throttle", "budget", "no_retry"}

type stepKind uint8

const (
	stCPU   stepKind = iota // the handler's own CPU
	stCall                  // a gRPC call through the service's LoadControl client
	stCache                 // memcached read, MongoDB for the misses, then fill
	stDB                    // a MongoDB operation (reservation insert)
)

type hstep struct {
	kind  stepKind
	to    *handler
	cache *cache
}

// handler is one RPC method: what it costs and what it calls, in order.
type handler struct {
	name  string
	svc   *service
	cpu   float64 // mean ms
	steps []hstep
}

type service struct {
	name   string
	id     int
	http   bool // the frontend speaks HTTP (lchttp), the rest gRPC (lcgrpc)
	leaf   bool
	cfg    lc.ServerConfig // DropExpired, MinBudget, DefaultTimeout, Pushback, OneLayer
	lim    *limiter
	dagor  *priority.Dagor
	sched  bool
	rl     *ratelimit.Bucket
	client [nSvc]*client

	maxConc, running int
	waitq            []func()

	proc     *proc // its Go process, for cpu=procs
	dbTODO   bool  // MongoDB queried with context.TODO(): never cancelled
	pool     int
	poolUsed int
	poolQ    []func()
}

type client struct{ cfg lc.ClientConfig }

type cache struct {
	name     string
	server   string // memcached instance
	query    string
	id       int
	present  []bool
	perReq   int
	disabled int64 // fills are dropped until this time (cold-cache trigger)
	proc     *proc // the memcached process
}

// info is the priority a request carries on every hop.
type info = priority.Info

// reply is what a caller learns from one attempt: the status and the
// trailers/headers LoadControl sets.
type reply struct {
	code     code
	shed     string // x-lc-shed
	pushback string // grpc-retry-pushback-ms
	noRetry  bool   // x-lc-no-retry
	local    bool   // produced by the caller's own stack, no response arrived
}

// serve is a request arriving at h's service from an attempt whose context
// is a. respond is called once with the reply.
func (s *Sim) serve(h *handler, a *ctx, in info, n int, respond func(reply)) {
	v, st := h.svc, s.sec()
	st.att[v.id][min(n-1, 1)]++
	if v.leaf {
		st.leaf++
	}
	inherit := !v.http || s.P.User.SendDeadline
	var c *ctx
	if s.P.CancelPropagation {
		c = s.newCtx(a, 0, inherit)
	} else {
		var dl int64
		if inherit {
			dl = a.deadline
		}
		c = s.newCtx(nil, dl, true)
	}
	if v.http && s.P.HiddenMS > 0 {
		s.run(s.host, s.work(s.P.HiddenMS), func() {})
	}
	s.run(v.proc, s.work(s.P.RPCServerMS), func() { s.admit(h, c, in, respond) })
}

// admit is Server.Admit: deadline check, rate limit, DAGOR, limiter.
func (s *Sim) admit(h *handler, base *ctx, in info, respond func(reply)) {
	v, cfg := h.svc, &h.svc.cfg
	c, dctx := base, (*ctx)(nil)
	if cfg.DefaultTimeout > 0 && c.deadline == 0 {
		dctx = s.newCtx(c, s.now+int64(cfg.DefaultTimeout), true)
		c = dctx
	}
	reject := func(reason int) {
		s.sec().shed[v.id][reason]++
		if dctx != nil {
			s.cancel(dctx, Canceled)
		}
		r := reply{code: ResourceExhausted, shed: shedNames[reason]}
		if reason == shedDeadline {
			r.code = DeadlineExceeded
		} else if cfg.Pushback != 0 {
			r.pushback = strconv.FormatInt(cfg.Pushback.Milliseconds(), 10)
		}
		respond(r)
	}
	if cfg.DropExpired && (c.deadline != 0 && c.deadline-s.now < int64(cfg.MinBudget) || c.err != OK) {
		reject(shedDeadline)
		return
	}
	if v.rl != nil && !v.rl.Allow() {
		reject(shedRate)
		return
	}
	if v.dagor != nil && !v.dagor.Admit(in) {
		reject(shedDagor)
		return
	}
	if v.lim == nil {
		s.handle(h, base, c, dctx, in, nil, respond)
		return
	}
	s.acquire(v.lim, int(in.Tier), c, func(t *token) {
		if t == nil {
			if cfg.DropExpired && c.err != OK {
				reject(shedDeadline)
			} else {
				reject(shedLimit)
			}
			return
		}
		if v.dagor != nil && !v.sched {
			v.dagor.ObserveDelay(time.Duration(t.wait))
		}
		if cfg.DropExpired && c.err != OK {
			s.release(t, false, false)
			reject(shedDeadline)
			return
		}
		s.handle(h, base, c, dctx, in, t, respond)
	})
}

// handle runs an admitted request and finishes its ticket the way the
// lcgrpc and lchttp adapters do.
func (s *Sim) handle(h *handler, base, c, dctx *ctx, in info, t *token, respond func(reply)) {
	v := h.svc
	start := func() {
		e := &exec{s: s, h: h, c: c, in: in}
		e.done = func(rc code) {
			if v.http && rc != OK {
				rc = Internal // the frontend answers 500 when a backend call fails
			}
			if t != nil {
				switch outcome(v.http, rc, c, base) {
				case lc.OK:
					s.release(t, true, false)
				case lc.Overload:
					s.release(t, true, true)
				default:
					s.release(t, false, false)
				}
			}
			if dctx != nil {
				s.cancel(dctx, Canceled)
			}
			if v.maxConc > 0 {
				s.slotFree(v)
			}
			respond(reply{code: rc, noRetry: v.cfg.OneLayer && rc != OK && e.df})
		}
		e.next()
	}
	if v.maxConc > 0 && v.running >= v.maxConc {
		v.waitq = append(v.waitq, func() {
			if c.err != OK { // the caller left while it waited: never dispatched
				s.slotFree(v)
				if t != nil {
					s.release(t, false, false)
				}
				if dctx != nil {
					s.cancel(dctx, Canceled)
				}
				respond(reply{code: c.err})
				return
			}
			start()
		})
		return
	}
	if v.maxConc > 0 {
		v.running++
	}
	start()
}

// outcome is lcgrpc's outcome() for gRPC and lchttp's status mapping for
// the HTTP frontend.
func outcome(http bool, rc code, c, base *ctx) lc.Outcome {
	if http {
		switch {
		case rc == ResourceExhausted || rc == DeadlineExceeded: // 503, 504
			return lc.Overload
		case c.err == DeadlineExceeded:
			return lc.Overload
		case base.err != OK:
			return lc.Ignore
		}
		return lc.OK
	}
	switch rc {
	case DeadlineExceeded, ResourceExhausted, Unavailable:
		return lc.Overload
	case Canceled:
		return lc.Ignore
	}
	return lc.OK
}

// exec walks a handler's steps. df is the per-request "a downstream call
// failed for good" flag (MarkDownstreamFailure). debt is CPU owed for
// decoding responses, paid with the next CPU step.
type exec struct {
	s    *Sim
	h    *handler
	c    *ctx
	in   info
	i    int
	df   bool
	debt float64
	done func(code)
}

func (e *exec) next() {
	s := e.s
	if e.i == len(e.h.steps) {
		e.finish(OK)
		return
	}
	st := e.h.steps[e.i]
	e.i++
	switch st.kind {
	case stCPU:
		w := s.work(e.h.cpu) + e.debt
		e.debt = 0
		s.run(e.h.svc.proc, w, e.next)
	case stCall:
		s.call(e, st.to)
	case stCache:
		s.cacheRead(e, st.cache)
	case stDB:
		s.dbOp(e, 1, e.next, e.finish)
	}
}

func (e *exec) finish(rc code) {
	if e.debt > 0 {
		d := e.debt
		e.debt = 0
		e.s.run(e.h.svc.proc, d, func() { e.done(rc) })
		return
	}
	e.done(rc)
}

// call is Client.Do around lcgrpc's invoker and classify, as events.
type call struct {
	e    *exec
	cl   *client
	to   *handler
	last reply
}

func (s *Sim) call(e *exec, to *handler) {
	k := &call{e: e, cl: e.h.svc.client[to.svc.id], to: to}
	if k.cl.cfg.Budget != nil {
		k.cl.cfg.Budget.OnRequest()
	}
	s.attempt(k, 1)
}

func (s *Sim) attempt(k *call, n int) {
	cfg, e := &k.cl.cfg, k.e
	if th := cfg.Throttle; th != nil {
		// Allow() is called for its bookkeeping (it counts the request);
		// the accept draw is redone on the seeded source with the same
		// probability so runs are reproducible.
		p := th.RejectProbability()
		th.Allow()
		if s.rMisc.Float64() < p {
			s.sec().local[localThrottle]++
			if n == 1 {
				k.last = reply{code: ResourceExhausted, local: true}
			}
			s.endCall(k)
			return
		}
	}
	a := e.c
	if cfg.PerTryTimeout > 0 {
		a = s.newCtx(e.c, s.now+int64(cfg.PerTryTimeout), true)
	}
	settled := false
	settle := func(r reply) {
		if settled {
			return
		}
		settled = true
		if a != e.c {
			s.cancel(a, Canceled)
		}
		s.post(func() { s.onReply(k, n, r) })
	}
	if a.err != OK {
		settle(reply{code: a.err, local: true})
		return
	}
	a.onDone(func() { settle(reply{code: a.err, local: true}) })
	s.at(s.now+ms(s.P.NetMS), func() { s.serve(k.to, a, e.in, n, settle) })
}

func (s *Sim) onReply(k *call, n int, r reply) {
	cfg, e := &k.cl.cfg, k.e
	if !r.local {
		e.debt += s.work(s.P.RPCClientMS)
	}
	if r.code == OK {
		if cfg.Throttle != nil {
			cfg.Throttle.Accepted()
		}
		if cfg.Budget != nil {
			cfg.Budget.OnResult(false)
		}
		e.next()
		return
	}
	k.last = r
	retryable := r.code == Unavailable || r.code == ResourceExhausted || (r.code == DeadlineExceeded && e.c.err == OK)
	if cfg.Throttle != nil && !(r.shed != "" && r.shed != "deadline") {
		cfg.Throttle.Accepted()
	}
	if cfg.Budget != nil && retryable { // gRFC A6: other failures leave the bucket alone
		cfg.Budget.OnResult(true)
	}
	if !retryable || n >= cfg.MaxAttempts || cfg.Budget == nil || e.c.err != OK {
		s.endCall(k)
		return
	}
	if cfg.HonorNoRetry && r.noRetry {
		s.sec().local[localNoRetry]++
		s.endCall(k)
		return
	}
	wait := s.backoff(cfg.Backoff, n)
	if cfg.HonorPushback {
		d, present, ok := retry.Pushback(r.pushback)
		if !ok {
			s.endCall(k)
			return
		}
		if present {
			wait = d
		}
	}
	if !cfg.Budget.AllowRetry() {
		s.sec().local[localBudget]++
		s.endCall(k)
		return
	}
	if wait <= 0 {
		s.attempt(k, n+1)
		return
	}
	fired := false
	s.at(s.now+int64(wait), func() {
		if !fired {
			fired = true
			s.attempt(k, n+1)
		}
	})
	e.c.onDone(func() {
		if !fired {
			fired = true
			s.post(func() { s.endCall(k) })
		}
	})
}

// endCall is the call failing for good: MarkDownstreamFailure, and the
// handler returns the error (hotelReservation handlers return it as is).
func (s *Sim) endCall(k *call) {
	k.e.df = true
	k.e.finish(k.last.code)
}

// backoff is retry.Backoff.Delay with the jitter drawn from the seeded
// source (the library draws from the global one).
func (s *Sim) backoff(b retry.Backoff, n int) time.Duration {
	if b.Initial <= 0 {
		return 0
	}
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(n-1))
	if b.Max > 0 && d > float64(b.Max) {
		d = float64(b.Max)
	}
	return time.Duration(s.rMisc.Float64() * d)
}

// cacheRead is the rate/profile/reservation pattern, as in the
// hotelReservation code: one memcached GetMulti for the request's keys (no
// context: it cannot be cancelled), MongoDB for the misses, then the
// handler goes on while a goroutine sets the keys in memcached. How the
// misses are queried follows each service: rate runs one query per missing
// hotel in sequence, profile one per hotel concurrently, reservation one
// $in query (capacities) or one per hotel and date concurrently (counts).
func (s *Sim) cacheRead(e *exec, ca *cache) {
	k := ca.perReq
	d := s.dep("memc-" + ca.server)
	s.run(ca.proc, s.work(s.P.MemcMS+s.P.MemcKeyMS*float64(k)), func() {
		s.at(s.now+d.extra, func() {
			st := s.sec()
			var miss []int
			for range k {
				key := s.rKey.IntN(len(ca.present))
				st.looks[ca.id]++
				if ca.present[key] && s.now >= ca.disabled && !d.down {
					st.hits[ca.id]++
				} else {
					miss = append(miss, key)
				}
			}
			if len(miss) == 0 {
				e.next()
				return
			}
			done := false
			fail := func(rc code) {
				if !done {
					done = true
					e.finish(rc)
				}
			}
			ok := func() {
				if done {
					return
				}
				done = true
				s.run(ca.proc, s.work(s.P.MemcMS), func() { // go MemcClient.Set
					s.at(s.now+d.extra, func() {
						if s.now >= ca.disabled && !d.down {
							for _, key := range miss {
								ca.present[key] = true
							}
						}
					})
				})
				e.next()
			}
			switch ca.query {
			case "sequential":
				var one func(i int)
				one = func(i int) {
					if i == len(miss) {
						ok()
						return
					}
					s.dbOp(e, 1, func() { one(i + 1) }, fail)
				}
				one(0)
			case "parallel":
				left := len(miss)
				for range miss {
					s.dbOp(e, 1, func() {
						if left--; left == 0 {
							ok()
						}
					}, fail)
				}
			default:
				s.dbOp(e, len(miss), ok, fail)
			}
		})
	})
}

// dbOp is one MongoDB operation from e's service: wait for a pooled
// connection, base I/O without CPU, the query's CPU in mongod, then any
// injected latency on the response. A request context abandons the wait
// (the driver returns ctx.Err()) but mongod finishes the work anyway; with
// context.TODO(), which every hotelReservation service uses, the handler
// waits it out.
func (s *Sim) dbOp(e *exec, keys int, then func(), fail func(code)) {
	v := e.h.svc
	s.sec().mongo++
	over := false
	if !v.dbTODO {
		if e.c.err != OK {
			fail(e.c.err)
			return
		}
		e.c.onDone(func() {
			if !over {
				over = true
				s.post(func() { fail(e.c.err) })
			}
		})
	}
	start := func() {
		if over && !v.dbTODO {
			s.poolRelease(v) // gave up while queued for a connection
			return
		}
		d := s.dep("mongo-" + v.name)
		if d.down { // connection refused: the handler returns the error
			s.poolRelease(v)
			if !over {
				over = true
				fail(Internal)
			}
			return
		}
		s.at(s.now+ms(s.P.Mongo.IOMS), func() {
			s.run(s.mongoProc[v.name], s.work(s.P.Mongo.OpMS+s.P.Mongo.KeyMS*float64(keys)), func() {
				s.at(s.now+d.extra, func() {
					s.poolRelease(v)
					if !over {
						over = true
						then()
					}
				})
			})
		})
	}
	if v.pool == 0 || v.poolUsed < v.pool {
		v.poolUsed++
		start()
		return
	}
	v.poolQ = append(v.poolQ, start)
}

func (s *Sim) poolRelease(v *service) {
	if len(v.poolQ) > 0 {
		next := v.poolQ[0]
		v.poolQ = v.poolQ[1:]
		s.post(next)
		return
	}
	v.poolUsed--
}

// slotFree ends one handler under max_concurrency and starts the next.
func (s *Sim) slotFree(v *service) {
	v.running--
	if len(v.waitq) > 0 {
		next := v.waitq[0]
		v.waitq = v.waitq[1:]
		v.running++
		s.post(next)
	}
}

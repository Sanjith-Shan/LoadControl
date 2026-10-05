package sim

import (
	"math"
	"time"
)

// The engine: a virtual clock in nanoseconds, a timer heap, a FIFO of
// work runnable at the current instant, and one processor-sharing CPU.
// Everything runs on one goroutine, so a run is a pure function of its
// params and seed.

type event struct {
	t   int64
	seq uint64
	fn  func()
}

type eventQ []event

func (q eventQ) less(i, j int) bool {
	return q[i].t < q[j].t || (q[i].t == q[j].t && q[i].seq < q[j].seq)
}

func (q *eventQ) push(e event) {
	*q = append(*q, e)
	h := *q
	for i := len(h) - 1; i > 0; {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func (q *eventQ) pop() event {
	h := *q
	top := h[0]
	n := len(h) - 1
	h[0] = h[n]
	h[n] = event{}
	h = h[:n]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && h.less(l, m) {
			m = l
		}
		if r < n && h.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
	*q = h
	return top
}

// epoch anchors the virtual clock handed to library components, so their
// wall-clock bucketing (throttle, ratio budget) behaves as in production.
var epoch = time.Unix(1_700_000_000, 0)

func (s *Sim) clock() time.Time { return epoch.Add(time.Duration(s.now)) }

func ms(x float64) int64 { return int64(x * 1e6) }

// at runs fn at time t (now if t is in the past).
func (s *Sim) at(t int64, fn func()) {
	if t <= s.now {
		s.post(fn)
		return
	}
	s.seq++
	s.q.push(event{t, s.seq, fn})
}

// post runs fn at the current instant, after work already posted. It is
// how callbacks avoid re-entering the code that triggered them, the way a
// goroutine woken by a channel runs later than the closer.
func (s *Sim) post(fn func()) { s.ready = append(s.ready, fn) }

// step runs the next instant's work; false when nothing is left.
func (s *Sim) step() bool {
	if s.rh < len(s.ready) {
		for s.rh < len(s.ready) {
			fn := s.ready[s.rh]
			s.ready[s.rh] = nil
			s.rh++
			s.events++
			fn()
		}
		s.ready, s.rh = s.ready[:0], 0
		return true
	}
	ct := s.cpu.next()
	if len(s.q) == 0 && ct == math.MaxInt64 {
		return false
	}
	if len(s.q) > 0 && s.q[0].t <= ct {
		e := s.q.pop()
		s.now = e.t
		s.events++
		e.fn()
		return true
	}
	s.now = ct
	s.cpu.complete(s)
	return true
}

// cpu shares C cores between jobs. In "ps" mode it is an egalitarian
// processor-sharing server: n jobs each progress at min(1, C/n). In
// "procs" mode it is two-level, like the real VM: every job belongs to a
// process (a Go service with GOMAXPROCS Ps, memcached or mongod with a
// few worker threads); a process runs at most slots jobs at once and
// queues the rest FIFO, as Go's run queues do, and preempts a running job
// after a slice of CPU if others wait; the kernel then shares the cores
// equally between all running jobs (threads), which is processor sharing
// over the running set. Both track v, the service every running job has
// received, so a job that joins at v with w left finishes when v reaches
// v+w and only the smallest finish point matters.
type cpu struct {
	cores   float64
	fcfs    bool // first come first served on C cores instead
	procs   bool // two-level: per-process FIFO slots, processor sharing between running jobs
	slice   float64
	runnext bool
	running int
	fifo    []cpuJob // FCFS waiting jobs; vf holds the work
	v       float64  // attained service per running job, ns
	last    int64
	busy    float64 // integral of min(n, C) dt, for utilization
	jobs    []cpuJob
	seq     uint64
	waiting int // queued inside processes
}

// proc is one process in "procs" mode.
type proc struct {
	name    string
	slots   int
	running int
	q       []cpuJob // waiting; end holds the work left
	preempt bool     // Go: preempt after a slice when others wait
	next    *cpuJob  // Go's runnext slot: the newest readied goroutine runs next
}

type cpuJob struct {
	vf   float64 // heap key: when this job next needs attention
	end  float64 // finish point (or, while queued, work left)
	seq  uint64
	p    *proc
	done func()
}

func (c *cpu) less(i, j int) bool {
	return c.jobs[i].vf < c.jobs[j].vf || (c.jobs[i].vf == c.jobs[j].vf && c.jobs[i].seq < c.jobs[j].seq)
}

func (c *cpu) advance(now int64) {
	if c.fcfs {
		c.busy += float64(now-c.last) * float64(c.running)
		c.last = now
		return
	}
	if n := float64(len(c.jobs)); n > 0 {
		dt := float64(now - c.last)
		c.v += dt * math.Min(1, c.cores/n)
		c.busy += dt * math.Min(n, c.cores)
	}
	c.last = now
}

// run adds a job of w nanoseconds of CPU in process p; done is posted
// when it finishes. p is ignored outside "procs" mode.
func (s *Sim) run(p *proc, w float64, done func()) {
	c := &s.cpu
	if w <= 0 {
		s.post(done)
		return
	}
	c.advance(s.now)
	if c.fcfs {
		if float64(c.running) < c.cores {
			c.running++
			s.at(s.now+int64(w), func() { s.fcfsDone(done) })
		} else {
			c.fifo = append(c.fifo, cpuJob{vf: w, done: done})
		}
		return
	}
	if !c.procs || p == nil {
		c.start(cpuJob{end: w, done: done})
		return
	}
	j := cpuJob{end: w, p: p, done: done}
	if p.running < p.slots {
		p.running++
		c.start(j)
		return
	}
	if p.preempt && c.runnext {
		// A goroutine readied by a running one (a new handler, a reply
		// handed to its caller) takes runnext; the one it displaces goes
		// to the back of the queue.
		if p.next != nil {
			p.q = append(p.q, *p.next)
		}
		p.next = &j
		c.waiting++
		return
	}
	p.q = append(p.q, j)
	c.waiting++
}

// start puts a job with j.end of work left on a core.
func (c *cpu) start(j cpuJob) {
	c.seq++
	j.seq = c.seq
	j.end += c.v
	j.vf = j.end
	if j.p != nil && j.p.preempt && c.slice > 0 {
		j.vf = math.Min(j.end, c.v+c.slice)
	}
	c.jobs = append(c.jobs, j)
	for i := len(c.jobs) - 1; i > 0; {
		p := (i - 1) / 2
		if !c.less(i, p) {
			break
		}
		c.jobs[i], c.jobs[p] = c.jobs[p], c.jobs[i]
		i = p
	}
}

func (c *cpu) pop() cpuJob {
	top := c.jobs[0]
	n := len(c.jobs) - 1
	c.jobs[0] = c.jobs[n]
	c.jobs[n] = cpuJob{}
	c.jobs = c.jobs[:n]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && c.less(l, m) {
			m = l
		}
		if r < n && c.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		c.jobs[i], c.jobs[m] = c.jobs[m], c.jobs[i]
		i = m
	}
	return top
}

func (c *cpu) next() int64 {
	if len(c.jobs) == 0 {
		return math.MaxInt64
	}
	rate := math.Min(1, c.cores/float64(len(c.jobs)))
	return c.last + int64(math.Ceil((c.jobs[0].vf-c.v)/rate))
}

func (c *cpu) complete(s *Sim) {
	c.advance(s.now)
	for len(c.jobs) > 0 && c.jobs[0].vf <= c.v+1e-3 {
		j := c.pop()
		p := j.p
		if j.end > c.v+1e-3 { // slice used up
			if len(p.q) == 0 && p.next == nil {
				c.start(cpuJob{end: j.end - c.v, p: p, done: j.done})
				continue
			}
			p.q = append(p.q, cpuJob{end: j.end - c.v, p: p, done: j.done})
			c.waiting++
			p.running--
		} else {
			s.post(j.done)
			if p == nil {
				continue
			}
			p.running--
		}
		if p.next != nil && p.running < p.slots {
			n := *p.next
			p.next = nil
			c.waiting--
			p.running++
			c.start(n)
		} else if len(p.q) > 0 && p.running < p.slots {
			n := p.q[0]
			p.q[0] = cpuJob{}
			p.q = p.q[1:]
			c.waiting--
			p.running++
			c.start(n)
		}
	}
}

// work draws a CPU demand with the given mean in ms, as nanoseconds.
func (s *Sim) work(mean float64) float64 {
	if mean <= 0 {
		return 0
	}
	m := mean * 1e6
	switch s.P.Dist {
	case "const":
		return m
	case "lognormal":
		sig := math.Sqrt(math.Log(1 + s.P.CV*s.P.CV))
		return m * math.Exp(sig*s.rSvc.NormFloat64()-sig*sig/2)
	}
	return m * s.rSvc.ExpFloat64()
}

// Status codes, named after gRPC's. HTTP statuses map onto them:
// 503 = ResourceExhausted, 504 = DeadlineExceeded, 500 = Internal.
type code uint8

const (
	OK code = iota
	Canceled
	DeadlineExceeded
	ResourceExhausted
	Unavailable
	Internal
)

// ctx mirrors context.Context: an effective deadline, an error once done,
// and cancellation that flows to children. hooks are the select arms of
// whoever is blocked on Done().
type ctx struct {
	deadline int64 // 0 = none
	err      code
	asCancel bool // a parent's end arrives as Canceled (an HTTP client hanging up)
	kids     []*ctx
	hooks    []func()
}

// newCtx derives a context with an absolute deadline (0 = none). inherit
// copies the parent's deadline (gRPC propagates it; plain HTTP does not,
// the server only sees the disconnect).
func (s *Sim) newCtx(parent *ctx, deadline int64, inherit bool) *ctx {
	c := &ctx{}
	if parent != nil {
		if inherit {
			c.deadline = parent.deadline
		} else {
			c.asCancel = true
		}
		if parent.err != OK {
			c.err = parent.err
			if c.asCancel {
				c.err = Canceled
			}
			return c
		}
		parent.kids = append(parent.kids, c)
	}
	if deadline > 0 && (c.deadline == 0 || deadline < c.deadline) {
		c.deadline = deadline
		s.at(deadline, func() { s.cancel(c, DeadlineExceeded) })
	}
	return c
}

func (s *Sim) cancel(c *ctx, err code) {
	if c.err != OK {
		return
	}
	c.err = err
	for _, h := range c.hooks {
		h()
	}
	for _, k := range c.kids {
		e := err
		if k.asCancel {
			e = Canceled
		}
		s.cancel(k, e)
	}
	c.hooks, c.kids = nil, nil
}

// onDone registers fn to run (synchronously) when c ends.
func (c *ctx) onDone(fn func()) { c.hooks = append(c.hooks, fn) }

func (s *Sim) fcfsDone(done func()) {
	c := &s.cpu
	c.advance(s.now)
	c.running--
	s.post(done)
	if len(c.fifo) > 0 {
		j := c.fifo[0]
		c.fifo[0] = cpuJob{}
		c.fifo = c.fifo[1:]
		c.running++
		s.at(s.now+int64(j.vf), func() { s.fcfsDone(j.done) })
	}
}

// load is the number of jobs on the CPU, running or waiting.
func (c *cpu) load() int { return len(c.jobs) + c.running + len(c.fifo) + c.waiting }

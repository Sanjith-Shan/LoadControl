package sim

import "math"

// cpu shares C cores between jobs, in one of three models.
//
//   - ps: one egalitarian processor-sharing pool; n jobs each progress at
//     min(1, C/n).
//   - fcfs: first come first served on C cores.
//   - procs: two levels, like the real VM. Every job belongs to a process
//     (a container: a Go service with GOMAXPROCS Ps, memcached or mongod
//     with a few worker threads; or a host process). A process runs at
//     most slots jobs at once and queues the rest FIFO, as Go's run queues
//     do, with Go's runnext slot and preemption after a slice of CPU. The
//     kernel shares the cores between busy processes by weight, as CFS
//     does between cgroups (Docker gives each container 1024): each gets
//     cores*w/W, at most one core per running thread, the excess going to
//     the others (water filling); a process's threads split its share.
//
// ps and procs both track virtual time: v, the service each running job
// of a pool (the whole CPU, or one process) has received, so a job that
// joins at v with w left finishes when v reaches v+w and only the
// smallest finish point matters.
type cpu struct {
	cores   float64
	fcfs    bool
	procs   bool
	slice   float64
	runnext bool
	running int      // fcfs
	fifo    []cpuJob // fcfs waiting jobs; vf holds the work
	v       float64  // ps: attained service per job, ns
	last    int64
	busy    float64 // integral of cores in use, for utilization
	jobs    heap    // ps
	seq     uint64
	all     []*proc // procs
	inUse   float64 // procs: cores allocated now
}

// proc is one process (container) in "procs" mode.
type proc struct {
	name    string
	slots   int
	weight  float64
	preempt bool    // Go: preempt after a slice when others wait
	hog     int     // busy-loop threads of a CPU hog fault
	next    *cpuJob // Go's runnext slot: the newest readied goroutine runs next
	q       []cpuJob
	jobs    heap    // running
	v       float64 // service received by each running job
	rate    float64 // progress per running job (cores)
}

type cpuJob struct {
	vf   float64 // heap key: when this job next needs attention
	end  float64 // finish point (or, while queued, work left)
	seq  uint64
	p    *proc
	done func()
}

type heap []cpuJob

func (h heap) less(i, j int) bool {
	return h[i].vf < h[j].vf || (h[i].vf == h[j].vf && h[i].seq < h[j].seq)
}

func (h *heap) push(j cpuJob) {
	*h = append(*h, j)
	q := *h
	for i := len(q) - 1; i > 0; {
		p := (i - 1) / 2
		if !q.less(i, p) {
			break
		}
		q[i], q[p] = q[p], q[i]
		i = p
	}
}

func (h *heap) pop() cpuJob {
	q := *h
	top := q[0]
	n := len(q) - 1
	q[0] = q[n]
	q[n] = cpuJob{}
	q = q[:n]
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && q.less(l, m) {
			m = l
		}
		if r < n && q.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		q[i], q[m] = q[m], q[i]
		i = m
	}
	*h = q
	return top
}

func (c *cpu) advance(now int64) {
	dt := float64(now - c.last)
	c.last = now
	switch {
	case c.fcfs:
		c.busy += dt * float64(c.running)
	case c.procs:
		for _, p := range c.all {
			if len(p.jobs) > 0 {
				p.v += dt * p.rate
			}
		}
		c.busy += dt * c.inUse
	default:
		if n := float64(len(c.jobs)); n > 0 {
			c.v += dt * math.Min(1, c.cores/n)
			c.busy += dt * math.Min(n, c.cores)
		}
	}
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
	switch {
	case c.fcfs:
		if float64(c.running) < c.cores {
			c.running++
			s.at(s.now+int64(w), func() { s.fcfsDone(done) })
		} else {
			c.fifo = append(c.fifo, cpuJob{vf: w, done: done})
		}
	case !c.procs || p == nil:
		c.seq++
		c.jobs.push(cpuJob{vf: c.v + w, seq: c.seq, done: done})
	default:
		j := cpuJob{end: w, p: p, done: done}
		switch {
		case len(p.jobs) < p.slots:
			c.start(p, j)
			c.share()
		case p.preempt && c.runnext:
			// A goroutine readied by a running one (a new handler, a reply
			// handed to its caller) takes runnext; the one it displaces goes
			// to the back of the queue.
			if p.next != nil {
				p.q = append(p.q, *p.next)
			}
			p.next = &j
		default:
			p.q = append(p.q, j)
		}
	}
}

// start puts a job with j.end of work left on one of p's threads.
func (c *cpu) start(p *proc, j cpuJob) {
	c.seq++
	j.seq = c.seq
	j.end += p.v
	j.vf = j.end
	if p.preempt && c.slice > 0 {
		j.vf = math.Min(j.end, p.v+c.slice)
	}
	p.jobs.push(j)
}

// share recomputes each process's CPU by weighted water filling.
func (c *cpu) share() {
	left, w := c.cores, 0.0
	var open []*proc
	for _, p := range c.all {
		p.rate = 0
		if d := len(p.jobs) + p.hog; d > 0 {
			open = append(open, p)
			w += p.weight
		}
	}
	c.inUse = 0
	for len(open) > 0 && left > 1e-12 {
		capped := false
		for i := 0; i < len(open); i++ {
			p := open[i]
			d := float64(len(p.jobs) + p.hog)
			if d <= left*p.weight/w {
				p.rate, left, w = 1, left-d, w-p.weight
				c.inUse += d
				open = append(open[:i], open[i+1:]...)
				i--
				capped = true
			}
		}
		if !capped {
			for _, p := range open {
				a := left * p.weight / w
				p.rate = a / float64(len(p.jobs)+p.hog)
				c.inUse += a
			}
			break
		}
	}
}

func (c *cpu) next() int64 {
	if c.procs {
		t := int64(math.MaxInt64)
		for _, p := range c.all {
			if len(p.jobs) > 0 && p.rate > 0 {
				t = min(t, c.last+int64(math.Ceil((p.jobs[0].vf-p.v)/p.rate)))
			}
		}
		return t
	}
	if len(c.jobs) == 0 {
		return math.MaxInt64
	}
	rate := math.Min(1, c.cores/float64(len(c.jobs)))
	return c.last + int64(math.Ceil((c.jobs[0].vf-c.v)/rate))
}

func (c *cpu) complete(s *Sim) {
	c.advance(s.now)
	if !c.procs {
		for len(c.jobs) > 0 && c.jobs[0].vf <= c.v+1e-3 {
			s.post(c.jobs.pop().done)
		}
		return
	}
	for _, p := range c.all {
		for len(p.jobs) > 0 && p.jobs[0].vf <= p.v+1e-3 {
			j := p.jobs.pop()
			if j.end > p.v+1e-3 { // slice used up
				left := cpuJob{end: j.end - p.v, p: p, done: j.done}
				if len(p.q) == 0 && p.next == nil {
					c.start(p, left)
					continue
				}
				p.q = append(p.q, left)
			} else {
				s.post(j.done)
			}
			switch {
			case p.next != nil:
				n := *p.next
				p.next = nil
				c.start(p, n)
			case len(p.q) > 0:
				n := p.q[0]
				p.q[0] = cpuJob{}
				p.q = p.q[1:]
				c.start(p, n)
			}
		}
	}
	c.share()
}

// setHog starts or stops n busy-loop threads in p (a CPU hog fault).
func (s *Sim) setHog(p *proc, n int) {
	s.cpu.advance(s.now)
	p.hog = n
	s.cpu.share()
}

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
func (c *cpu) load() int {
	n := len(c.jobs) + c.running + len(c.fifo)
	for _, p := range c.all {
		n += len(p.jobs) + len(p.q)
		if p.next != nil {
			n++
		}
	}
	return n
}

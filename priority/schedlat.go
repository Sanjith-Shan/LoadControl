package priority

import (
	"runtime/metrics"
	"sync"
	"time"
)

// SchedLatency samples the Go runtime's goroutine scheduling latency
// histogram (/sched/latencies:seconds): how long runnable goroutines waited
// for a CPU. A Go gRPC server has no request queue of its own (every
// request gets a goroutine at once), so this is the closest thing it has to
// the queueing delay DAGOR and Breakwater watch. Mean returns the mean
// latency of goroutines scheduled since the previous call.
type SchedLatency struct {
	mu      sync.Mutex
	sample  []metrics.Sample
	prev    []uint64
	buckets []float64
}

const schedMetric = "/sched/latencies:seconds"

func NewSchedLatency() *SchedLatency {
	s := &SchedLatency{sample: []metrics.Sample{{Name: schedMetric}}}
	metrics.Read(s.sample)
	if s.sample[0].Value.Kind() == metrics.KindFloat64Histogram {
		h := s.sample[0].Value.Float64Histogram()
		s.prev = append([]uint64(nil), h.Counts...)
		s.buckets = h.Buckets
	}
	return s
}

// Mean returns the mean scheduling latency since the last call, and the
// number of scheduling events it covers.
func (s *SchedLatency) Mean() (time.Duration, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metrics.Read(s.sample)
	if s.sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0, 0
	}
	h := s.sample[0].Value.Float64Histogram()
	var n uint64
	var sum float64
	for i, c := range h.Counts {
		var d uint64
		if i < len(s.prev) {
			d = c - s.prev[i]
		} else {
			d = c
		}
		if d == 0 {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		if lo < 0 || lo != lo { // -Inf
			lo = 0
		}
		if hi > 1e9 { // +Inf
			hi = lo
		}
		sum += float64(d) * (lo + hi) / 2
		n += d
	}
	s.prev = append(s.prev[:0], h.Counts...)
	if n == 0 {
		return 0, 0
	}
	return time.Duration(sum / float64(n) * float64(time.Second)), n
}

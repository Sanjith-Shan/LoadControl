package loadcontrol

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the Prometheus series the Grafana dashboard reads. One set
// per process; Server and Client label by service and target.
type Metrics struct {
	Requests       *prometheus.CounterVec   // service, tier, outcome
	Latency        *prometheus.HistogramVec // service, tier (admitted requests only)
	QueueWait      *prometheus.HistogramVec // service
	Limit          *prometheus.GaugeVec     // service
	Inflight       *prometheus.GaugeVec     // service
	DagorLevel     *prometheus.GaugeVec     // service
	ClientAttempts *prometheus.CounterVec   // service, target, kind (original|retry)
	ClientLocal    *prometheus.CounterVec   // service, target, reason (throttle|budget|no_retry)
	ClientFailures *prometheus.CounterVec   // service, target
	RetryTokens    *prometheus.GaugeVec     // service, target
	ThrottleProb   *prometheus.GaugeVec     // service, target
	Inbound        *prometheus.CounterVec   // service, kind (original|retry): requests received, before admission
}

var (
	defaultMetricsOnce sync.Once
	defaultMetrics     *Metrics
)

// DefaultMetrics returns a Metrics registered on the default Prometheus
// registry (created once).
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() { defaultMetrics = NewMetrics(prometheus.DefaultRegisterer) })
	return defaultMetrics
}

// NewMetrics creates and registers the series on reg (nil: unregistered).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	lat := []float64{.001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5}
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lc_requests_total",
			Help: "Server admission decisions by tier and outcome."}, []string{"service", "tier", "outcome"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lc_server_latency_seconds",
			Help: "Latency of admitted requests.", Buckets: lat}, []string{"service", "tier"}),
		QueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lc_queue_wait_seconds",
			Help: "Time spent waiting for a concurrency slot.", Buckets: lat}, []string{"service"}),
		Limit:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lc_limit", Help: "Current concurrency limit."}, []string{"service"}),
		Inflight:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lc_inflight", Help: "Admitted requests in flight."}, []string{"service"}),
		DagorLevel: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lc_dagor_level", Help: "DAGOR admission level as a compound key."}, []string{"service"}),
		ClientAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lc_client_attempts_total",
			Help: "Outbound attempts by kind."}, []string{"service", "target", "kind"}),
		ClientLocal: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lc_client_local_total",
			Help: "Outbound requests or retries stopped on the client."}, []string{"service", "target", "reason"}),
		ClientFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lc_client_failures_total",
			Help: "Outbound calls that failed after all attempts."}, []string{"service", "target"}),
		RetryTokens:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lc_retry_tokens", Help: "Retry budget tokens."}, []string{"service", "target"}),
		ThrottleProb: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lc_throttle_probability", Help: "Client adaptive throttle reject probability."}, []string{"service", "target"}),
		Inbound: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lc_inbound_total",
			Help: "Requests received by kind, before admission."}, []string{"service", "kind"}),
	}
	if reg != nil {
		reg.MustRegister(m.Requests, m.Latency, m.QueueWait, m.Limit, m.Inflight, m.DagorLevel,
			m.ClientAttempts, m.ClientLocal, m.ClientFailures, m.RetryTokens, m.ThrottleProb, m.Inbound)
	}
	return m
}

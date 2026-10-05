package loadcontrol

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/ratelimit"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"github.com/Sanjith-Shan/LoadControl/throttle"
)

// Env reads configuration from environment variables so the benchmark can
// switch pieces per run without rebuilding. For key K and service S it
// looks up LC_<S>_<K> first, then LC_<K>. S is upper-cased with dashes
// turned into underscores.
//
//	LIMIT            none | fixed:N | aimd | vegas | gradient2
//	LIMIT_INITIAL    initial limit (default 20)
//	LIMIT_MIN/MAX    bounds (defaults 4 / 500)
//	LIMIT_TIMEOUT_MS AIMD: latency counted as a drop (default 500)
//	QUEUE_WAIT_MS    how long a request may wait for a slot (default 0)
//	TIER_SHARES      e.g. "1,0.9,0.7": share of the limit per tier; empty = no tiers
//	DAGOR            off | wait | sched
//	DAGOR_THRESHOLD_MS  (default 20)
//	DEADLINE         on | off: drop requests whose deadline has passed
//	MIN_BUDGET_MS    remaining time below which a request is dropped (default 1)
//	DEFAULT_TIMEOUT_MS  deadline given to requests that arrive without one
//	RATELIMIT        static rate limit in requests/s (0 = off)
//	PUSHBACK_MS      pushback sent with overload rejections (-1 = do not retry)
//	ONE_LAYER        on | off
//	RETRY            none | naive | budget | ratio
//	RETRY_ATTEMPTS   total attempts (default 3)
//	RETRY_TOKENS     budget max tokens (default 10); RETRY_RATIO token ratio (0.1)
//	PER_TRY_TIMEOUT_MS  per-attempt timeout (0 = none)
//	BACKOFF_MS       initial backoff (default 0)
//	THROTTLE         off | K (e.g. 2)
//	THROTTLE_WINDOW_S   throttle history (default 120)
//	THROTTLE_FAILURES   on: count timeouts and other retryable failures as rejections
type Env struct {
	Service string
	Lookup  func(string) (string, bool)
}

// FromEnv returns an Env over the process environment.
func FromEnv(service string) Env { return Env{Service: service, Lookup: os.LookupEnv} }

func (e Env) get(key string) string {
	svc := strings.ToUpper(strings.ReplaceAll(e.Service, "-", "_"))
	if v, ok := e.Lookup("LC_" + svc + "_" + key); ok {
		return v
	}
	v, _ := e.Lookup("LC_" + key)
	return v
}

func (e Env) str(key, def string) string {
	if v := e.get(key); v != "" {
		return v
	}
	return def
}

func (e Env) num(key string, def float64) float64 {
	v := e.get(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func (e Env) ms(key string, def float64) time.Duration {
	return time.Duration(e.num(key, def) * float64(time.Millisecond))
}

func (e Env) on(key string) bool {
	switch strings.ToLower(e.get(key)) {
	case "on", "1", "true", "yes":
		return true
	}
	return false
}

// Algorithm builds the configured limit algorithm, or nil.
func (e Env) Algorithm() (limit.Algorithm, error) {
	spec := e.str("LIMIT", "none")
	initial := int(e.num("LIMIT_INITIAL", 20))
	minL, maxL := int(e.num("LIMIT_MIN", 4)), int(e.num("LIMIT_MAX", 500))
	switch {
	case spec == "none" || spec == "off":
		return nil, nil
	case strings.HasPrefix(spec, "fixed:"):
		n, err := strconv.Atoi(strings.TrimPrefix(spec, "fixed:"))
		if err != nil {
			return nil, fmt.Errorf("LC_LIMIT %q: %v", spec, err)
		}
		return &limit.Fixed{N: n}, nil
	case spec == "aimd":
		a := limit.NewAIMD(initial)
		a.MinLimit, a.MaxLimit = minL, maxL
		a.Timeout = e.ms("LIMIT_TIMEOUT_MS", 500)
		return a, nil
	case spec == "vegas":
		v := limit.NewVegas(initial)
		v.MaxLimit = maxL
		return v, nil
	case spec == "gradient2":
		g := limit.NewGradient2(initial)
		g.MinLimit, g.MaxLimit = minL, maxL
		return g, nil
	}
	return nil, fmt.Errorf("LC_LIMIT %q: unknown algorithm", spec)
}

func parseShares(s string) ([]float64, error) {
	if s == "" {
		return nil, nil
	}
	var out []float64
	for _, p := range strings.Split(s, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("LC_TIER_SHARES %q: %v", s, err)
		}
		out = append(out, f)
	}
	return out, nil
}

// ServerConfig builds the server configuration.
func (e Env) ServerConfig(m *Metrics) (ServerConfig, error) {
	cfg := ServerConfig{Name: e.Service, Metrics: m}
	alg, err := e.Algorithm()
	if err != nil {
		return cfg, err
	}
	shares, err := parseShares(e.get("TIER_SHARES"))
	if err != nil {
		return cfg, err
	}
	if alg != nil {
		cfg.Limiter = limit.NewLimiter(alg, limit.Options{Shares: shares, MaxWait: e.ms("QUEUE_WAIT_MS", 0)})
	}
	switch d := e.str("DAGOR", "off"); d {
	case "off":
	case "wait", "sched":
		cfg.Dagor = priority.NewDagor(nil)
		cfg.Dagor.Threshold = e.ms("DAGOR_THRESHOLD_MS", 20)
		cfg.DagorSignal = d
	default:
		return cfg, fmt.Errorf("LC_DAGOR %q: want off, wait or sched", d)
	}
	cfg.DropExpired = e.on("DEADLINE")
	cfg.MinBudget = e.ms("MIN_BUDGET_MS", 1)
	cfg.DefaultTimeout = e.ms("DEFAULT_TIMEOUT_MS", 0)
	if r := e.num("RATELIMIT", 0); r > 0 {
		cfg.RateLimit = ratelimit.New(r, e.num("RATELIMIT_BURST", r/10+1), nil)
	}
	cfg.Pushback = e.ms("PUSHBACK_MS", 0)
	cfg.OneLayer = e.on("ONE_LAYER")
	return cfg, nil
}

// ClientConfig builds the client configuration for calls to target.
func (e Env) ClientConfig(target string, m *Metrics) (ClientConfig, error) {
	cfg := ClientConfig{Service: e.Service, Target: target, Metrics: m}
	switch r := e.str("RETRY", "none"); r {
	case "none", "off":
	case "naive":
		cfg.Budget = retry.Unlimited{}
	case "budget":
		cfg.Budget = retry.NewBudget(e.num("RETRY_TOKENS", 10), e.num("RETRY_RATIO", 0.1))
	case "ratio":
		cfg.Budget = retry.NewRatioBudget(e.num("RETRY_RATIO", 0.1), e.num("RETRY_MIN_PER_SEC", 1), 10*time.Second, nil)
	default:
		return cfg, fmt.Errorf("LC_RETRY %q: unknown", r)
	}
	cfg.MaxAttempts = int(e.num("RETRY_ATTEMPTS", 3))
	cfg.PerTryTimeout = e.ms("PER_TRY_TIMEOUT_MS", 0)
	cfg.Backoff = retry.Backoff{Initial: e.ms("BACKOFF_MS", 0), Max: e.ms("BACKOFF_MAX_MS", 1000), Multiplier: 2}
	cfg.HonorPushback = cfg.Budget != nil && e.str("RETRY", "") != "naive"
	cfg.HonorNoRetry = e.on("ONE_LAYER")
	if k := e.str("THROTTLE", "off"); k != "off" {
		f, err := strconv.ParseFloat(k, 64)
		if err != nil {
			return cfg, fmt.Errorf("LC_THROTTLE %q: %v", k, err)
		}
		cfg.Throttle = throttle.New(f, time.Duration(e.num("THROTTLE_WINDOW_S", 120))*time.Second, nil)
		cfg.ThrottleOnFailure = e.on("THROTTLE_FAILURES")
	}
	return cfg, nil
}

// Describe summarizes the effective configuration for logs and results.
func (e Env) Describe() map[string]string {
	keys := []string{"LIMIT", "QUEUE_WAIT_MS", "TIER_SHARES", "DAGOR", "DEADLINE", "DEFAULT_TIMEOUT_MS",
		"RATELIMIT", "PUSHBACK_MS", "ONE_LAYER", "RETRY", "RETRY_ATTEMPTS", "PER_TRY_TIMEOUT_MS", "THROTTLE"}
	out := map[string]string{}
	for _, k := range keys {
		if v := e.get(k); v != "" {
			out[k] = v
		}
	}
	return out
}

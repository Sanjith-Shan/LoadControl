package sim

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed params.calibrated.json
var defaultParams []byte

//go:embed params.json
var placeholderParams []byte

// Params is everything a run needs: the model (CPU costs, caches, MongoDB),
// the workload, the fault triggers and the LoadControl configuration as
// the same env-style keys the library reads (LIMIT, RETRY, ...).
type Params struct {
	Notes []string `json:"_notes,omitempty"`

	Seed      uint64  `json:"seed"`
	DurationS float64 `json:"duration_s"` // arrivals stop here; the run drains after
	WarmupS   float64 `json:"warmup_s"`   // excluded from the summary

	Cores float64 `json:"cores"` // the CPU every container shares
	CPU   string  `json:"cpu"`   // ps (processor sharing) | procs (per-process FIFO slots, then processor sharing) | fcfs
	// procs mode: worker slots per process kind (go = GOMAXPROCS of each
	// service, memcached and mongod threads, host = the load generator and
	// other CPU outside the services) and the Go preemption slice.
	Slots     map[string]int `json:"slots,omitempty"`
	SliceMS   float64        `json:"slice_ms,omitempty"`
	NoRunnext bool           `json:"no_runnext,omitempty"` // procs mode: plain FIFO, no runnext slot
	// CFS weight per process kind (Docker cpu-shares; default 1024).
	Weights    map[string]float64 `json:"weights,omitempty"`
	HogwWeight float64            `json:"hogw_weight,omitempty"` // cpu-shares of the hogw fault container (default 20480)
	Dist       string             `json:"dist"`                  // exp | lognormal | const
	CV         float64            `json:"cv"`                    // lognormal coefficient of variation

	CapacityRPS float64 `json:"capacity_rps"` // measured max goodput with no control; load.x is relative to it

	NetMS       float64            `json:"net_ms"`        // non-CPU latency per hop (one way, added on the request)
	RPCServerMS float64            `json:"rpc_server_ms"` // CPU to receive an RPC, paid before admission (so shed requests cost it too)
	RPCClientMS float64            `json:"rpc_client_ms"` // CPU at the caller per response received
	HiddenMS    float64            `json:"hidden_ms"`     // CPU per user attempt off the critical path (kernel, GC, tracing)
	Handlers    map[string]float64 `json:"handlers"`      // mean handler CPU ms, by service.method

	Caches    map[string]CacheParams `json:"caches"`
	MemcMS    float64                `json:"memc_ms"`     // memcached CPU per operation
	MemcKeyMS float64                `json:"memc_key_ms"` // plus per key
	Mongo     MongoParams            `json:"mongo"`
	Conn      ConnParams             `json:"conn"`

	MaxConcurrency    map[string]int `json:"max_concurrency"`    // optional per-service handler cap; 0 = a goroutine per request
	CancelPropagation bool           `json:"cancel_propagation"` // caller cancel reaches the callee (gRPC, HTTP disconnect)
	SchedQuantumMS    float64        `json:"sched_quantum_ms"`   // for DAGOR=sched: run-queue delay per excess runnable job

	SLOMS float64            `json:"slo_ms"`
	User  UserParams         `json:"user"`
	Mix   map[string]float64 `json:"mix"`   // search, recommend, user, reserve
	Tiers []float64          `json:"tiers"` // share of user requests per tier (critical, default, sheddable)
	Load  LoadParams         `json:"load"`

	Flush Trigger `json:"flush"` // cold cache: flushed at At, fills discarded until At+Dur
	Slow  Trigger `json:"slow"`  // MongoDB latency injection (toxiproxy)
	// Faults is a timeline in bench/lcbench.py's spec language:
	// flush:<rate|profile|reserve>, latency:<proxy>:<ms>, down:<proxy>,
	// up:<proxy>, clear. Proxies are mongo-<svc> and memc-<cache>.
	Faults []Fault `json:"faults,omitempty"`

	Env map[string]string `json:"env"` // LoadControl keys, LC_ prefix optional, LC_<SVC>_<KEY> per service
}

type CacheParams struct {
	Keys   int    `json:"keys"`             // key space
	PerReq int    `json:"per_req"`          // keys read per request (one GetMulti)
	Server string `json:"server,omitempty"` // memcached instance (flush and latency target); default the cache name
	Query  string `json:"query,omitempty"`  // misses as: single (one query), sequential or parallel (one per key)
}

// ConnParams model the HTTP connections between the load generator and the
// frontend. The load generator keeps idle keep-alive connections; an
// attempt that times out closes its connection, so the next one needs a new
// one. Connections to the frontend's published port go through Docker's
// userland proxy, which accepts, dials the container and copies, as a Go
// process of its own.
type ConnParams struct {
	Proxy         bool    `json:"proxy"`           // model docker-proxy and connection reuse
	NewMS         float64 `json:"new_ms"`          // docker-proxy CPU per new connection (accept, dial, goroutines, teardown)
	ReqMS         float64 `json:"req_ms"`          // docker-proxy CPU per request copied
	FrontendNewMS float64 `json:"frontend_new_ms"` // frontend CPU to accept a connection
	MaxIdle       int     `json:"max_idle"`        // the load generator's idle connection cap (MaxIdleConnsPerHost)
}

type MongoParams struct {
	OpMS    float64         `json:"op_ms"`  // CPU per query
	KeyMS   float64         `json:"key_ms"` // plus per document
	IOMS    float64         `json:"io_ms"`  // non-CPU latency per query
	Pool    int             `json:"pool"`   // driver connection pool per service; 0 = unbounded
	CtxTODO map[string]bool `json:"ctx_todo"`
}

type UserParams struct {
	Users         int     `json:"users"`
	TimeoutMS     float64 `json:"timeout_ms"`
	Retries       int     `json:"retries"`
	BackoffMS     float64 `json:"backoff_ms"` // initial, exponential with full jitter, x2, capped at 1 s
	HonorNoRetry  bool    `json:"honor_no_retry"`
	HonorPushback bool    `json:"honor_pushback"`
	SendDeadline  bool    `json:"send_deadline"` // send X-Lc-Deadline-Ms so the frontend has a deadline
	RetryShed     bool    `json:"retry_shed"`    // retry 503s from admission control (loadgen default)
}

type LoadParams struct {
	Process  string  `json:"process"` // poisson | constant
	RPS      float64 `json:"rps"`     // absolute rate; if 0, X*capacity_rps
	X        float64 `json:"x"`
	Steps    string  `json:"steps"`    // "t:x;t:x" piecewise constant multiples of capacity, overrides rps/x
	Schedule string  `json:"schedule"` // loadgen format "0s:500,30s:1500" in req/s, overrides all of the above
	// A warm-up phase before t=0 (lcbench runs 15 s at 100 req/s): it
	// adapts limits and budgets and is not reported.
	WarmupS   float64 `json:"warmup_s"`
	WarmupRPS float64 `json:"warmup_rps"`
}

type Fault struct {
	At   float64 `json:"t"`
	Spec string  `json:"fault"`
}

type Trigger struct {
	At      float64  `json:"at"`
	Dur     float64  `json:"dur"`
	ExtraMS float64  `json:"extra_ms,omitempty"`
	Targets []string `json:"targets,omitempty"` // empty = all
}

func (t Trigger) hits(name string) bool {
	if len(t.Targets) == 0 {
		return true
	}
	for _, x := range t.Targets {
		if x == name {
			return true
		}
	}
	return false
}

// Default returns the embedded sim/params.calibrated.json, fitted to the
// measured runs (see README.md, Calibration).
func Default() *Params { return mustParse(defaultParams) }

// Placeholder returns the embedded sim/params.json: the uncalibrated
// starting point, with guessed costs.
func Placeholder() *Params { return mustParse(placeholderParams) }

func mustParse(b []byte) *Params {
	p, err := parse(b)
	if err != nil {
		panic(err)
	}
	return p
}

// Load reads a params file; fields it omits keep the defaults.
func Load(path string) (*Params, error) {
	if path == "" {
		return Default(), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := Default()
	if err := json.Unmarshal(b, p); err != nil {
		return nil, fmt.Errorf("%s: %v", path, err)
	}
	return p, nil
}

func parse(b []byte) (*Params, error) {
	p := &Params{}
	return p, json.Unmarshal(b, p)
}

// Clone deep-copies p.
func (p *Params) Clone() *Params {
	b, _ := json.Marshal(p)
	q, _ := parse(b)
	return q
}

// Set applies one override. Upper-case keys are LoadControl env keys
// (LIMIT=gradient2, FRONTEND_RETRY=naive, LC_DEADLINE=on). Lower-case keys
// are dotted paths into the params (user.retries=2, handlers.geo.nearby=0.3,
// load.x=3); the value is parsed as JSON if it can be, else as a string.
func (p *Params) Set(key, val string) error {
	if key == "" {
		return nil
	}
	if strings.ToUpper(key) == key {
		if !strings.HasPrefix(key, "LC_") {
			key = "LC_" + key
		}
		if p.Env == nil {
			p.Env = map[string]string{}
		}
		p.Env[key] = val
		return nil
	}
	var m map[string]any
	b, _ := json.Marshal(p)
	json.Unmarshal(b, &m)
	var v any
	if err := json.Unmarshal([]byte(val), &v); err != nil {
		v = val
	}
	if err := setPath(m, key, v); err != nil {
		return err
	}
	b, _ = json.Marshal(m)
	q, err := parse(b)
	if err != nil {
		return fmt.Errorf("%s=%s: %v", key, val, err)
	}
	*p = *q
	return nil
}

// setPath sets a dotted path, matching the longest existing key at each
// level so map keys that contain dots (handler names) work.
func setPath(m map[string]any, path string, v any) error {
	parts := strings.Split(path, ".")
	for i := len(parts); i >= 1; i-- {
		k := strings.Join(parts[:i], ".")
		cur, ok := m[k]
		if i == len(parts) {
			if ok || len(parts) == 1 {
				m[k] = v
				return nil
			}
			continue
		}
		if sub, isMap := cur.(map[string]any); ok && isMap {
			return setPath(sub, strings.Join(parts[i:], "."), v)
		}
		if !ok && i == 1 {
			break
		}
	}
	// New key in an existing map (e.g. a new max_concurrency entry).
	if len(parts) > 1 {
		if sub, ok := m[parts[0]].(map[string]any); ok {
			sub[strings.Join(parts[1:], ".")] = v
			return nil
		}
		if m[parts[0]] == nil {
			m[parts[0]] = map[string]any{strings.Join(parts[1:], "."): v}
			return nil
		}
	}
	return fmt.Errorf("unknown parameter %q", path)
}

// ApplyConfig applies "k=v,k=v". A comma-separated piece without "="
// continues the previous value, so TIER_SHARES=1,0.9,0.7 works.
func (p *Params) ApplyConfig(s string) error {
	for _, kv := range SplitConfig(s) {
		if err := p.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

// SplitConfig parses "k=v,k=v" as ApplyConfig does.
func SplitConfig(s string) [][2]string {
	var out [][2]string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok && len(out) > 0 {
			out[len(out)-1][1] += "," + part
			continue
		}
		out = append(out, [2]string{strings.TrimSpace(k), strings.TrimSpace(v)})
	}
	return out
}

// EnvSummary renders the LoadControl keys as "K=v ..." sorted, for labels.
func (p *Params) EnvSummary() string {
	keys := make([]string, 0, len(p.Env))
	for k := range p.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strings.TrimPrefix(k, "LC_") + "=" + p.Env[k])
	}
	return b.String()
}

type step struct{ t, x float64 }

func (p *Params) steps() ([]step, error) {
	if p.Load.Schedule != "" {
		var out []step
		for _, s := range strings.Split(p.Load.Schedule, ",") {
			a, b, _ := strings.Cut(strings.TrimSpace(s), ":")
			t, e1 := time.ParseDuration(a)
			r, e2 := strconv.ParseFloat(b, 64)
			if e1 != nil || e2 != nil {
				return nil, fmt.Errorf("load.schedule %q: want 0s:500,30s:1500", p.Load.Schedule)
			}
			out = append(out, step{t.Seconds(), r})
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].t < out[j].t })
		return out, nil
	}
	base := p.Load.RPS
	if base <= 0 {
		base = p.Load.X * p.CapacityRPS
	}
	if p.Load.Steps == "" {
		return []step{{0, base}}, nil
	}
	var out []step
	for _, s := range strings.Split(p.Load.Steps, ";") {
		a, b, ok := strings.Cut(strings.TrimSpace(s), ":")
		t, e1 := strconv.ParseFloat(a, 64)
		x, e2 := strconv.ParseFloat(b, 64)
		if !ok || e1 != nil || e2 != nil {
			return nil, fmt.Errorf("load.steps %q: want t:x;t:x", p.Load.Steps)
		}
		out = append(out, step{t, x * p.CapacityRPS})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].t < out[j].t })
	return out, nil
}

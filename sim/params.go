package sim

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

//go:embed params.json
var defaultParams []byte

// Params is everything a run needs: the model (CPU costs, caches, MongoDB),
// the workload, the fault triggers and the LoadControl configuration as
// the same env-style keys the library reads (LIMIT, RETRY, ...).
type Params struct {
	Notes []string `json:"_notes,omitempty"`

	Seed      uint64  `json:"seed"`
	DurationS float64 `json:"duration_s"` // arrivals stop here; the run drains after
	WarmupS   float64 `json:"warmup_s"`   // excluded from the summary

	Cores float64 `json:"cores"` // the CPU every container shares
	CPU   string  `json:"cpu"`   // ps (processor sharing, default) | fcfs
	Dist  string  `json:"dist"`  // exp | lognormal | const
	CV    float64 `json:"cv"`    // lognormal coefficient of variation

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

	Env map[string]string `json:"env"` // LoadControl keys, LC_ prefix optional, LC_<SVC>_<KEY> per service
}

type CacheParams struct {
	Keys   int `json:"keys"`    // key space
	PerReq int `json:"per_req"` // keys read per request
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
}

type LoadParams struct {
	Process string  `json:"process"` // poisson | constant
	RPS     float64 `json:"rps"`     // absolute rate; if 0, X*capacity_rps
	X       float64 `json:"x"`
	Steps   string  `json:"steps"` // "t:x;t:x" piecewise constant multiples of capacity, overrides rps/x
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

// Default returns the embedded sim/params.json.
func Default() *Params {
	p, err := parse(defaultParams)
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

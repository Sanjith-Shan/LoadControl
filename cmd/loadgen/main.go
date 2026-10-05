// Command loadgen is an open-loop HTTP load generator for the
// hotelReservation frontend. It sends the same request mix as the
// benchmark's wrk2 script (mixed-workload_type_1.lua: 60% search,
// 39% recommend, 0.5% login, 0.5% reserve) on a fixed schedule that does
// not slow down when the server does, and measures latency from each
// request's intended send time, as wrk2 does, so coordinated omission does
// not hide queueing.
//
// Beyond wrk2 it models the users: each request carries a tier and a user
// id, and a user request that fails or times out can be retried the way a
// mobile client would. Goodput counts user requests that succeeded within
// the SLO, once each, however many attempts that took.
//
// Output is JSON lines: one per second plus a summary.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type step struct {
	at   time.Duration
	rate float64
}

// parseSchedule reads "0s:500,30s:1500,90s:500" (time offset:rate).
func parseSchedule(s string) ([]step, error) {
	var out []step
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad schedule step %q", part)
		}
		at, err := time.ParseDuration(kv[0])
		if err != nil {
			return nil, err
		}
		r, err := strconv.ParseFloat(kv[1], 64)
		if err != nil {
			return nil, err
		}
		out = append(out, step{at, r})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at < out[j].at })
	return out, nil
}

func rateAt(s []step, t time.Duration) float64 {
	r := 0.0
	for _, st := range s {
		if t >= st.at {
			r = st.rate
		}
	}
	return r
}

type kind int

const (
	kSearch kind = iota
	kRecommend
	kLogin
	kReserve
	numKinds
)

var kindNames = [numKinds]string{"search", "recommend", "login", "reserve"}

// request builds a URL following the wrk2 Lua script's distributions.
func request(base string, r *rand.Rand) (kind, string, string) {
	date := func(d int) string { return fmt.Sprintf("2015-04-%02d", d) }
	lat := 38.0235 + (float64(r.IntN(482))-240.5)/1000.0
	lon := -122.095 + (float64(r.IntN(326))-157.0)/1000.0
	coin := r.Float64()
	switch {
	case coin < 0.6:
		in := 9 + r.IntN(15)
		out := in + 1 + r.IntN(24-in)
		return kSearch, "GET", fmt.Sprintf("%s/hotels?inDate=%s&outDate=%s&lat=%g&lon=%g", base, date(in), date(out), lat, lon)
	case coin < 0.99:
		req := [3]string{"dis", "rate", "price"}[r.IntN(3)]
		return kRecommend, "GET", fmt.Sprintf("%s/recommendations?require=%s&lat=%g&lon=%g", base, req, lat, lon)
	case coin < 0.995:
		id := r.IntN(501)
		return kLogin, "POST", fmt.Sprintf("%s/user?username=Cornell_%d&password=%s", base, id, strings.Repeat(strconv.Itoa(id), 10))
	default:
		in := 9 + r.IntN(15)
		out := in + 1 + r.IntN(5)
		id := r.IntN(501)
		return kReserve, "POST", fmt.Sprintf("%s/reservation?inDate=%s&outDate=%s&lat=%g&lon=%g&hotelId=%d&customerName=Cornell_%d&username=Cornell_%d&password=%s&number=1",
			base, date(in), date(out), lat, lon, 1+r.IntN(80), id, id, strings.Repeat(strconv.Itoa(id), 10))
	}
}

// outcome of one user request
const (
	oGood    = iota // 2xx within SLO
	oSlow           // 2xx but over SLO
	oShed           // final attempt rejected by admission control (503 + X-Lc-Shed)
	oError          // other 5xx / transport error
	oTimeout        // client timeout on the final attempt
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"good", "slow", "shed", "error", "timeout"}

type sec struct {
	offered  int64 // user requests scheduled to start in this second
	attempts int64 // attempts sent in this second (original + retries)
	retries  int64
	outcome  [numOutcomes]int64 // by completion second
	tierDone [8][numOutcomes]int64
	lat      []float64 // ms, successful requests completing in this second
	tierLat  [8][]float64
	kindLat  [numKinds][]float64
	lateSend int64 // attempts sent more than 10 ms after their intended time
}

type stats struct {
	mu   sync.Mutex
	secs []*sec
}

func (s *stats) at(i int) *sec {
	if i < 0 {
		i = 0
	}
	for len(s.secs) <= i {
		s.secs = append(s.secs, &sec{})
	}
	return s.secs[i]
}

type config struct {
	base         string
	timeout      time.Duration
	retries      int
	backoff      time.Duration
	honorNoRetry bool
	honorRetryAf bool
	retryOnShed  bool
	slo          time.Duration
	tiers        []float64
	users        int
	deadlineHdr  bool
}

func pickTier(r *rand.Rand, shares []float64) int {
	x := r.Float64()
	for i, s := range shares {
		if x < s {
			return i
		}
		x -= s
	}
	return len(shares) - 1
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return s[i]
}

func main() {
	var (
		target    = flag.String("target", "http://localhost:5000", "frontend base URL")
		rate      = flag.Float64("rate", 100, "constant request rate (ignored if -schedule set)")
		schedule  = flag.String("schedule", "", "rate schedule, e.g. 0s:500,30s:1500,90s:500")
		duration  = flag.Duration("duration", 60*time.Second, "run length")
		timeout   = flag.Duration("timeout", time.Second, "client timeout per attempt")
		retries   = flag.Int("retries", 0, "user-level retries after a failed attempt")
		backoff   = flag.Duration("backoff", 0, "base backoff before a user retry (jittered)")
		noRetry   = flag.Bool("honor-no-retry", false, "do not retry responses marked X-Lc-No-Retry")
		retryAf   = flag.Bool("honor-retry-after", false, "wait Retry-After / pushback before retrying a shed request")
		retryShed = flag.Bool("retry-shed", true, "retry 503 responses from admission control")
		slo       = flag.Duration("slo", 500*time.Millisecond, "latency SLO for goodput")
		tiersFlag = flag.String("tiers", "0.2,0.3,0.5", "share of user requests per tier (0 = critical)")
		users     = flag.Int("users", 1000, "simulated user pool for DAGOR user priority")
		poisson   = flag.Bool("poisson", false, "Poisson arrivals instead of evenly spaced")
		deadline  = flag.Bool("deadline-header", false, "send X-Lc-Deadline-Ms equal to the client timeout")
		out       = flag.String("out", "", "JSONL output file (default stdout)")
		label     = flag.String("label", "", "free-form label copied into every line")
		seed      = flag.Uint64("seed", 1, "random seed")
		maxConns  = flag.Int("conns", 2000, "max idle connections")
	)
	flag.Parse()

	var sched []step
	if *schedule != "" {
		var err error
		if sched, err = parseSchedule(*schedule); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	} else {
		sched = []step{{0, *rate}}
	}
	var tiers []float64
	for _, p := range strings.Split(*tiersFlag, ",") {
		f, _ := strconv.ParseFloat(p, 64)
		tiers = append(tiers, f)
	}
	cfg := config{base: *target, timeout: *timeout, retries: *retries, backoff: *backoff,
		honorNoRetry: *noRetry, honorRetryAf: *retryAf, retryOnShed: *retryShed, slo: *slo,
		tiers: tiers, users: *users, deadlineHdr: *deadline}

	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        *maxConns,
		MaxIdleConnsPerHost: *maxConns,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Transport: tr}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		w = f
	}

	st := &stats{}
	var wg sync.WaitGroup
	var inflight atomic.Int64
	var maxInflight atomic.Int64
	start := time.Now().Add(50 * time.Millisecond)
	rnd := rand.New(rand.NewPCG(*seed, 7))

	// Scheduler: compute intended start times and launch each user request
	// on its own goroutine at that time.
	next := time.Duration(0)
	var n int64
	for next < *duration {
		r := rateAt(sched, next)
		if r <= 0 {
			next += 10 * time.Millisecond
			continue
		}
		gap := time.Duration(float64(time.Second) / r)
		if *poisson {
			gap = time.Duration(rnd.ExpFloat64() * float64(time.Second) / r)
		}
		intended := start.Add(next)
		next += gap
		if d := time.Until(intended); d > 0 {
			time.Sleep(d)
		}
		k, method, url := request(cfg.base, rnd)
		tier := pickTier(rnd, cfg.tiers)
		user := rnd.IntN(cfg.users)
		jit := rnd.Uint64()
		n++
		st.mu.Lock()
		st.at(int(intended.Sub(start) / time.Second)).offered++
		st.mu.Unlock()
		wg.Add(1)
		cur := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if cur <= m || maxInflight.CompareAndSwap(m, cur) {
				break
			}
		}
		go func() {
			defer wg.Done()
			defer inflight.Add(-1)
			userRequest(client, cfg, st, start, intended, k, method, url, tier, user, jit)
		}()
	}
	wg.Wait()

	enc := json.NewEncoder(w)
	host, _ := os.Hostname()
	var all [numOutcomes]int64
	var tierAll [8][numOutcomes]int64
	var tierLat [8][]float64
	var kindLat [numKinds][]float64
	var lat []float64
	var attempts, retriesSent, offered, late int64
	for i, s := range st.secs {
		row := map[string]any{"type": "second", "t": i, "label": *label, "offered": s.offered,
			"attempts": s.attempts, "retries": s.retries, "late_sends": s.lateSend,
			"p50_ms": nanToNil(percentile(s.lat, 50)), "p99_ms": nanToNil(percentile(s.lat, 99))}
		for o := 0; o < numOutcomes; o++ {
			row[outcomeNames[o]] = s.outcome[o]
			all[o] += s.outcome[o]
		}
		tiersRow := map[string]any{}
		for t := range cfg.tiers {
			tr := map[string]any{}
			for o := 0; o < numOutcomes; o++ {
				tr[outcomeNames[o]] = s.tierDone[t][o]
				tierAll[t][o] += s.tierDone[t][o]
			}
			tr["p99_ms"] = nanToNil(percentile(s.tierLat[t], 99))
			tiersRow[strconv.Itoa(t)] = tr
			tierLat[t] = append(tierLat[t], s.tierLat[t]...)
		}
		row["tiers"] = tiersRow
		lat = append(lat, s.lat...)
		for k := range kindLat {
			kindLat[k] = append(kindLat[k], s.kindLat[k]...)
		}
		attempts += s.attempts
		retriesSent += s.retries
		offered += s.offered
		late += s.lateSend
		enc.Encode(row)
	}
	sum := map[string]any{"type": "summary", "label": *label, "host": host, "target": cfg.base,
		"schedule": *schedule, "rate": *rate, "duration_s": duration.Seconds(), "timeout_ms": cfg.timeout.Milliseconds(),
		"retries": cfg.retries, "backoff_ms": cfg.backoff.Milliseconds(), "honor_no_retry": cfg.honorNoRetry,
		"honor_retry_after": cfg.honorRetryAf, "slo_ms": cfg.slo.Milliseconds(), "tiers": cfg.tiers,
		"offered": offered, "attempts": attempts, "retries_sent": retriesSent, "late_sends": late,
		"max_inflight": maxInflight.Load(), "seed": *seed,
		"p50_ms": nanToNil(percentile(lat, 50)), "p99_ms": nanToNil(percentile(lat, 99)),
		"p999_ms": nanToNil(percentile(lat, 99.9))}
	for o := 0; o < numOutcomes; o++ {
		sum[outcomeNames[o]] = all[o]
	}
	tsum := map[string]any{}
	for t := range cfg.tiers {
		m := map[string]any{"p50_ms": nanToNil(percentile(tierLat[t], 50)), "p99_ms": nanToNil(percentile(tierLat[t], 99))}
		var tot int64
		for o := 0; o < numOutcomes; o++ {
			m[outcomeNames[o]] = tierAll[t][o]
			tot += tierAll[t][o]
		}
		if tot > 0 {
			m["success_rate"] = float64(tierAll[t][oGood]+tierAll[t][oSlow]) / float64(tot)
		}
		tsum[strconv.Itoa(t)] = m
	}
	sum["tiers_summary"] = tsum
	ksum := map[string]any{}
	for k := range kindLat {
		ksum[kindNames[k]] = map[string]any{"n": len(kindLat[k]), "mean_ms": nanToNil(mean(kindLat[k])),
			"p50_ms": nanToNil(percentile(kindLat[k], 50)), "p99_ms": nanToNil(percentile(kindLat[k], 99))}
	}
	sum["kinds_summary"] = ksum
	sum["mean_ms"] = nanToNil(mean(lat))
	enc.Encode(sum)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	t := 0.0
	for _, x := range xs {
		t += x
	}
	return t / float64(len(xs))
}

func nanToNil(f float64) any {
	if math.IsNaN(f) {
		return nil
	}
	return math.Round(f*1000) / 1000
}

func userRequest(client *http.Client, cfg config, st *stats, start, intended time.Time, k kind, method, url string, tier, user int, jit uint64) {
	sendAt := intended
	result := oError
	for attempt := 0; ; attempt++ {
		if d := time.Until(sendAt); d > 0 {
			time.Sleep(d)
		}
		now := time.Now()
		st.mu.Lock()
		s := st.at(int(now.Sub(start) / time.Second))
		s.attempts++
		if attempt > 0 {
			s.retries++
		}
		if now.Sub(sendAt) > 10*time.Millisecond {
			s.lateSend++
		}
		st.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		req, _ := http.NewRequestWithContext(ctx, method, url, nil)
		req.Header.Set("X-Lc-Tier", strconv.Itoa(tier))
		req.Header.Set("X-Lc-User", strconv.Itoa(user))
		req.Header.Set("X-Lc-Attempt", strconv.Itoa(attempt+1))
		if cfg.deadlineHdr {
			req.Header.Set("X-Lc-Deadline-Ms", strconv.FormatInt(cfg.timeout.Milliseconds(), 10))
		}
		resp, err := client.Do(req)
		retryable, wait := false, time.Duration(0)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				result = oTimeout
			} else {
				result = oError
			}
			retryable = true
		default:
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			switch {
			case resp.StatusCode < 300:
				if time.Since(intended) <= cfg.slo {
					result = oGood
				} else {
					result = oSlow
				}
			case resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get("X-Lc-Shed") != "":
				result = oShed
				retryable = cfg.retryOnShed
				if cfg.honorRetryAf {
					if v := resp.Header.Get("X-Lc-Retry-Pushback-Ms"); v != "" {
						ms, _ := strconv.Atoi(v)
						if ms < 0 {
							retryable = false
						}
						wait = time.Duration(ms) * time.Millisecond
					}
				}
			default:
				result = oError
				retryable = resp.StatusCode >= 500
			}
			if cfg.honorNoRetry && resp.Header.Get("X-Lc-No-Retry") != "" {
				retryable = false
			}
		}
		cancel()
		if result == oGood || result == oSlow || !retryable || attempt >= cfg.retries {
			break
		}
		if cfg.backoff > 0 {
			// full jitter, exponential
			b := cfg.backoff << attempt
			jit = jit*6364136223846793005 + 1442695040888963407
			wait += time.Duration(float64(b) * float64(jit>>11) / float64(1<<53))
		}
		sendAt = time.Now().Add(wait)
	}
	done := time.Now()
	st.mu.Lock()
	s := st.at(int(done.Sub(start) / time.Second))
	s.outcome[result]++
	if tier < 8 {
		s.tierDone[tier][result]++
	}
	if result == oGood || result == oSlow {
		ms := float64(done.Sub(intended).Microseconds()) / 1000
		s.lat = append(s.lat, ms)
		s.kindLat[k] = append(s.kindLat[k], ms)
		if tier < 8 {
			s.tierLat[tier] = append(s.tierLat[tier], ms)
		}
	}
	st.mu.Unlock()
}

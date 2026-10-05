package lchttp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
)

func naive(cfg lc.ClientConfig) *http.Client {
	if cfg.Budget == nil {
		cfg.Budget = retry.Unlimited{}
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	return &http.Client{Transport: &Transport{Client: lc.NewClient(cfg)}}
}

// backend counts requests and answers with codes[i], then the last code.
type backend struct {
	mu      sync.Mutex
	codes   []int
	headers []http.Header
	bodies  []string
}

func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	i := min(len(b.headers), len(b.codes)-1)
	b.headers = append(b.headers, r.Header.Clone())
	b.bodies = append(b.bodies, string(body))
	code := b.codes[i]
	b.mu.Unlock()
	w.WriteHeader(code)
	io.WriteString(w, "body")
}

func (b *backend) count() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.headers) }

func TestShed503(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	s := lc.NewServer(lc.ServerConfig{Limiter: limit.NewLimiter(&limit.Fixed{N: 1}, limit.Options{}), Pushback: 1500 * time.Millisecond})
	srv := httptest.NewServer(Middleware(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	})))
	defer srv.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if res, err := http.Get(srv.URL); err == nil {
			res.Body.Close()
		}
	}()
	<-entered
	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	close(release)
	<-done
	if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get(retry.ShedHeader) != "limit" {
		t.Fatalf("status %d shed %q", res.StatusCode, res.Header.Get(retry.ShedHeader))
	}
	if res.Header.Get("Retry-After") != "2" || res.Header.Get(PushbackHeader) != "1500" {
		t.Fatalf("Retry-After %q pushback %q", res.Header.Get("Retry-After"), res.Header.Get(PushbackHeader))
	}
}

func TestExpiredDeadline504(t *testing.T) {
	var calls atomic.Int64
	s := lc.NewServer(lc.ServerConfig{DropExpired: true, MinBudget: time.Millisecond})
	srv := httptest.NewServer(Middleware(s, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) })))
	defer srv.Close()
	for _, v := range []string{"0", "-20"} {
		r, _ := http.NewRequest("GET", srv.URL, nil)
		r.Header.Set(DeadlineHeader, v)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusGatewayTimeout || res.Header.Get(retry.ShedHeader) != "deadline" {
			t.Fatalf("deadline %s: status %d shed %q", v, res.StatusCode, res.Header.Get(retry.ShedHeader))
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times", calls.Load())
	}
}

// A 5xx caused by a downstream call that failed after its retries carries
// X-Lc-No-Retry when the one-layer rule is on, and not otherwise.
func TestNoRetryMarker(t *testing.T) {
	for _, oneLayer := range []bool{false, true} {
		down := &backend{codes: []int{http.StatusServiceUnavailable}}
		dsrv := httptest.NewServer(down)
		client := naive(lc.ClientConfig{HonorNoRetry: true})
		front := httptest.NewServer(Middleware(lc.NewServer(lc.ServerConfig{OneLayer: oneLayer}), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req, _ := http.NewRequestWithContext(r.Context(), "GET", dsrv.URL, nil)
			res, err := client.Do(req)
			if err == nil {
				res.Body.Close()
				if res.StatusCode < 500 {
					return
				}
			}
			http.Error(w, "downstream failed", http.StatusInternalServerError)
		})))
		res, err := http.Get(front.URL)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		front.Close()
		dsrv.Close()
		if res.StatusCode != 500 || (res.Header.Get(retry.NoRetryHeader) != "") != oneLayer {
			t.Fatalf("oneLayer=%v: status %d no-retry %q", oneLayer, res.StatusCode, res.Header.Get(retry.NoRetryHeader))
		}
		if down.count() != 3 {
			t.Fatalf("downstream saw %d attempts, want 3", down.count())
		}
	}
}

// The transport retries 503s within budget and propagates priority,
// deadline and attempt number.
func TestTransportRetries(t *testing.T) {
	b := &backend{codes: []int{503, 503, 200}}
	srv := httptest.NewServer(b)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(priority.WithInfo(context.Background(), priority.Info{Tier: priority.Critical, User: 5}), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL, strings.NewReader("payload"))
	res, err := naive(lc.ClientConfig{Budget: retry.NewBudget(10, 0.1)}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 || b.count() != 3 {
		t.Fatalf("status %d after %d attempts", res.StatusCode, b.count())
	}
	for i, h := range b.headers {
		ms, _ := strconv.Atoi(h.Get(DeadlineHeader))
		if h.Get(retry.AttemptHeader) != strconv.Itoa(i+1) || h.Get(priority.Header) != "0" || h.Get(priority.UserHeader) != "5" ||
			ms <= 0 || ms > 10000 || b.bodies[i] != "payload" {
			t.Fatalf("attempt %d: headers %v body %q", i+1, h, b.bodies[i])
		}
	}

	// Against a backend that always fails, the budget allows 3 retries over
	// 20 calls (10 tokens: 9, 8 retried; 7 out of attempts; 6 retried; 5 stops).
	b = &backend{codes: []int{503}}
	srv2 := httptest.NewServer(b)
	defer srv2.Close()
	c := naive(lc.ClientConfig{Budget: retry.NewBudget(10, 0.1)})
	for i := 0; i < 20; i++ {
		res, err := c.Get(srv2.URL)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}
	if b.count() != 23 {
		t.Fatalf("%d requests for 20 calls, want 23", b.count())
	}
}

// The response returned to the caller must stay readable after the attempt
// loop ends, also when attempts have their own timeout.
func TestTransportBodyReadable(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%2 == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush() // the body arrives after the headers
		time.Sleep(20 * time.Millisecond)
		io.WriteString(w, "body")
	}))
	defer srv.Close()
	for _, perTry := range []time.Duration{0, 5 * time.Second} {
		res, err := naive(lc.ClientConfig{PerTryTimeout: perTry}).Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || string(body) != "body" {
			t.Fatalf("per-try %v: body %q err %v", perTry, body, err)
		}
	}
}

// Package lchttp adapts LoadControl to net/http: a server middleware and a
// client RoundTripper.
//
// Wire format: X-Lc-Tier and X-Lc-User carry priority, X-Lc-Deadline-Ms
// carries the caller's remaining time budget in milliseconds (HTTP has no
// standard deadline header, so this plays the role of grpc-timeout).
// Overload rejections are 503 with X-Lc-Shed and, if configured,
// Retry-After plus X-Lc-Retry-Pushback-Ms; expired deadlines are 504.
// A 5xx caused by a failed downstream call carries X-Lc-No-Retry when the
// one-layer rule is on.
package lchttp

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
)

const (
	DeadlineHeader = "X-Lc-Deadline-Ms"
	PushbackHeader = "X-Lc-Retry-Pushback-Ms"
)

func infoFromRequest(r *http.Request) priority.Info {
	info := priority.Info{Tier: priority.Parse(r.Header.Get(priority.Header)), User: -1}
	if u := r.Header.Get(priority.UserHeader); u != "" {
		if n, err := strconv.Atoi(u); err == nil && n >= 0 && n < priority.UserLevels {
			info.User = n
		} else {
			info.User = priority.UserPriority(u, uint64(time.Now().Unix()/3600))
		}
	}
	if info.User < 0 {
		info.User = priority.RandomUser()
	}
	return info
}

type statusWriter struct {
	http.ResponseWriter
	status   int
	wrote    bool
	t        *lc.Ticket
	oneLayer bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = code
	if code >= 500 && w.oneLayer && w.t != nil && w.t.DownstreamFailed() {
		w.Header().Set(retry.NoRetryHeader, "1")
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Middleware wraps next with admission control. s may be nil (priority
// and deadline propagation only).
func Middleware(s *lc.Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if v := r.Header.Get(DeadlineHeader); v != "" {
			if ms, err := strconv.ParseFloat(v, 64); err == nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(ms*float64(time.Millisecond)))
				defer cancel()
			}
		}
		info := infoFromRequest(r)
		if s == nil {
			next.ServeHTTP(w, r.WithContext(priority.WithInfo(ctx, info)))
			return
		}
		actx, t, rej := s.Admit(ctx, info)
		if rej != nil {
			w.Header().Set(retry.ShedHeader, rej.Reason)
			code := http.StatusServiceUnavailable
			if rej.Reason == "deadline" {
				code = http.StatusGatewayTimeout
			}
			if rej.Pushback > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(rej.Pushback.Seconds()))))
				w.Header().Set(PushbackHeader, strconv.FormatInt(rej.Pushback.Milliseconds(), 10))
			} else if rej.Pushback < 0 {
				w.Header().Set(PushbackHeader, "-1")
			}
			w.WriteHeader(code)
			return
		}
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK, t: t, oneLayer: s.Config().OneLayer}
		next.ServeHTTP(sw, r.WithContext(actx))
		o := lc.OK
		switch {
		case sw.status == http.StatusServiceUnavailable || sw.status == http.StatusGatewayTimeout:
			o = lc.Overload
		case actx.Err() != nil && errors.Is(context.Cause(actx), context.DeadlineExceeded):
			o = lc.Overload
		case r.Context().Err() != nil:
			o = lc.Ignore
		}
		t.Done(o, sw.status >= 500)
	})
}

// Transport is a RoundTripper that propagates priority and deadline and
// runs the throttle and retry loop. Requests with bodies are retried only
// if GetBody is set.
type Transport struct {
	Base   http.RoundTripper
	Client *lc.Client
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	prep := func(r *http.Request, n int) {
		if p, ok := priority.FromContext(ctx); ok {
			r.Header.Set(priority.Header, strconv.Itoa(int(p.Tier)))
			r.Header.Set(priority.UserHeader, strconv.Itoa(p.User))
		}
		if dl, ok := r.Context().Deadline(); ok {
			r.Header.Set(DeadlineHeader, strconv.FormatInt(time.Until(dl).Milliseconds(), 10))
		}
		r.Header.Set(retry.AttemptHeader, strconv.Itoa(n))
	}
	if t.Client == nil {
		r := req.Clone(ctx)
		prep(r, 1)
		return t.base().RoundTrip(r)
	}
	var resp *http.Response
	err := t.Client.Do(ctx, func(actx context.Context, n int) lc.Attempt {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			resp = nil
		}
		r := req.Clone(actx)
		if n > 1 && req.Body != nil {
			if req.GetBody == nil {
				return lc.Attempt{Err: errors.New("lchttp: body not replayable")}
			}
			b, err := req.GetBody()
			if err != nil {
				return lc.Attempt{Err: err}
			}
			r.Body = b
		}
		prep(r, n)
		res, err := t.base().RoundTrip(r)
		if err != nil {
			return lc.Attempt{Err: err, Retryable: ctx.Err() == nil}
		}
		resp = res
		if res.StatusCode < 500 && res.StatusCode != http.StatusTooManyRequests {
			return lc.Attempt{}
		}
		a := lc.Attempt{Err: errors.New("lchttp: " + res.Status)}
		switch res.StatusCode {
		case http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusBadGateway:
			a.Retryable = true
		case http.StatusGatewayTimeout:
			a.Retryable = ctx.Err() == nil
		}
		if v := res.Header.Get(retry.ShedHeader); v != "" && v != "deadline" {
			a.Overloaded = true
		}
		a.NoRetry = res.Header.Get(retry.NoRetryHeader) != ""
		a.Pushback = res.Header.Get(PushbackHeader)
		return a
	})
	if errors.Is(err, lc.ErrThrottled) {
		return nil, err
	}
	if resp != nil {
		// Return the last response, failed or not, as net/http clients
		// expect a response for HTTP-level errors.
		return resp, nil
	}
	return nil, err
}

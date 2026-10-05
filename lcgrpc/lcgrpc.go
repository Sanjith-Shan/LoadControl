// Package lcgrpc adapts LoadControl to gRPC-Go as unary and streaming
// interceptors.
//
// Server side: priority is read from metadata, the request goes through
// the admission pipeline, and overload rejections come back as
// RESOURCE_EXHAUSTED (deadline drops as DEADLINE_EXCEEDED) with an
// x-lc-shed trailer and optional grpc-retry-pushback-ms.
//
// Client side: priority is copied into outgoing metadata, the adaptive
// throttle and the retry loop run around the call, and gRPC's own
// deadline propagation carries the deadline to the next hop.
package lcgrpc

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// userEpoch rotates DAGOR user priorities hourly.
func userEpoch() uint64 { return uint64(time.Now().Unix() / 3600) }

// AttemptCounter, if set, counts inbound requests per attempt number, so
// the experiments can measure retries per hop at the server.
type AttemptCounter struct{ Original, Retry atomic.Int64 }

// infoFromMD extracts priority from incoming metadata.
func infoFromMD(ctx context.Context) priority.Info {
	if p, ok := priority.FromContext(ctx); ok {
		return p
	}
	info := priority.Info{Tier: priority.Default, User: -1}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(priority.Header); len(v) > 0 {
			info.Tier = priority.Parse(v[0])
		}
		if v := md.Get(priority.UserHeader); len(v) > 0 {
			if n, err := strconv.Atoi(v[0]); err == nil && n >= 0 && n < priority.UserLevels {
				info.User = n
			} else {
				info.User = priority.UserPriority(v[0], userEpoch())
			}
		}
	}
	if info.User < 0 {
		info.User = priority.RandomUser()
	}
	return info
}

func rejectErr(ctx context.Context, r *lc.Rejection) error {
	tr := metadata.Pairs(retry.ShedHeader, r.Reason)
	if r.Pushback != 0 {
		tr.Append(retry.PushbackHeader, strconv.FormatInt(r.Pushback.Milliseconds(), 10))
	}
	_ = grpc.SetTrailer(ctx, tr)
	if r.Reason == "deadline" {
		return status.Error(codes.DeadlineExceeded, r.Error())
	}
	return status.Error(codes.ResourceExhausted, r.Error())
}

func outcome(err error) lc.Outcome {
	if err == nil {
		return lc.OK
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Unavailable:
		return lc.Overload
	case codes.Canceled:
		return lc.Ignore
	}
	return lc.OK
}

// UnaryServerInterceptor runs admission control. s may be nil, in which
// case only priority propagation happens.
func UnaryServerInterceptor(s *lc.Server, counter *AttemptCounter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		countAttempt(ctx, s, counter)
		p := infoFromMD(ctx)
		if s == nil {
			return handler(priority.WithInfo(ctx, p), req)
		}
		actx, t, rej := s.Admit(ctx, p)
		if rej != nil {
			return nil, rejectErr(ctx, rej)
		}
		resp, err := handler(actx, req)
		if t.Done(outcome(err), err != nil) {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(retry.NoRetryHeader, "1"))
		}
		return resp, err
	}
}

func countAttempt(ctx context.Context, s *lc.Server, c *AttemptCounter) {
	var m *lc.Metrics
	if s != nil {
		m = s.Config().Metrics
	}
	if c == nil && m == nil {
		return
	}
	kind := "original"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(retry.AttemptHeader); len(v) > 0 && v[0] != "1" {
			kind = "retry"
		}
	}
	if c != nil {
		if kind == "retry" {
			c.Retry.Add(1)
		} else {
			c.Original.Add(1)
		}
	}
	if m != nil {
		m.Inbound.WithLabelValues(s.Config().Name, kind).Inc()
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// StreamServerInterceptor admits a stream once, at its start; the slot is
// held for the stream's lifetime.
func StreamServerInterceptor(s *lc.Server, counter *AttemptCounter) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		countAttempt(ctx, s, counter)
		p := infoFromMD(ctx)
		if s == nil {
			return handler(srv, &wrappedStream{ss, priority.WithInfo(ctx, p)})
		}
		actx, t, rej := s.Admit(ctx, p)
		if rej != nil {
			return rejectErr(ctx, rej)
		}
		err := handler(srv, &wrappedStream{ss, actx})
		if t.Done(outcome(err), err != nil) {
			ss.SetTrailer(metadata.Pairs(retry.NoRetryHeader, "1"))
		}
		return err
	}
}

// outgoing adds priority metadata for the next hop.
func outgoing(ctx context.Context) context.Context {
	p, ok := priority.FromContext(ctx)
	if !ok {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx,
		priority.Header, strconv.Itoa(int(p.Tier)),
		priority.UserHeader, strconv.Itoa(p.User))
}

// classify turns one attempt's error and trailer into an Attempt.
func classify(parent context.Context, err error, tr metadata.MD) lc.Attempt {
	a := lc.Attempt{Err: err}
	if err == nil {
		return a
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable, codes.ResourceExhausted:
		a.Retryable = true
	case codes.DeadlineExceeded:
		// Only a per-try timeout is worth retrying; if the caller's own
		// deadline is gone there is nothing left to retry with.
		a.Retryable = parent.Err() == nil
	}
	if v := tr.Get(retry.ShedHeader); len(v) > 0 && v[0] != "deadline" {
		a.Overloaded = true
	}
	if len(tr.Get(retry.NoRetryHeader)) > 0 {
		a.NoRetry = true
	}
	if v := tr.Get(retry.PushbackHeader); len(v) > 0 {
		a.Pushback = v[0]
	}
	return a
}

// UnaryClientInterceptor runs throttle and retries for calls on a conn.
// c may be nil (priority propagation only).
func UnaryClientInterceptor(c *lc.Client) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = outgoing(ctx)
		if c == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		err := c.Do(ctx, func(actx context.Context, n int) lc.Attempt {
			var tr metadata.MD
			actx = metadata.AppendToOutgoingContext(actx, retry.AttemptHeader, strconv.Itoa(n))
			err := invoker(actx, method, req, reply, cc, append(opts, grpc.Trailer(&tr))...)
			return classify(ctx, err, tr)
		})
		if err == lc.ErrThrottled {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return err
	}
}

// StreamClientInterceptor propagates priority and applies the adaptive
// throttle to stream creation. Streams are not retried: a retry after
// messages were exchanged is not safe to do transparently.
func StreamClientInterceptor(c *lc.Client) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = outgoing(ctx)
		if c != nil {
			if t := c.Config().Throttle; t != nil && !t.Allow() {
				lc.MarkDownstreamFailure(ctx)
				return nil, status.Error(codes.ResourceExhausted, lc.ErrThrottled.Error())
			}
		}
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if c != nil {
			if t := c.Config().Throttle; t != nil && err == nil {
				t.Accepted()
			}
		}
		return cs, err
	}
}

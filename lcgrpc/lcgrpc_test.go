package lcgrpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/internal/leakcheck"
	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestMain(m *testing.M) { leakcheck.Main(m) }

// health is the test service: the standard health Check RPC with a
// pluggable handler body, so tests control latency and errors.
type health struct {
	grpc_health_v1.UnimplementedHealthServer
	calls atomic.Int64
	fn    func(ctx context.Context) error
}

func (h *health) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	h.calls.Add(1)
	if h.fn != nil {
		if err := h.fn(ctx); err != nil {
			return nil, err
		}
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (h *health) Watch(_ *grpc_health_v1.HealthCheckRequest, ss grpc_health_v1.Health_WatchServer) error {
	h.calls.Add(1)
	return ss.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING})
}

// serve starts h on lis, behind the LoadControl interceptors unless bare.
func serve(tb testing.TB, lis net.Listener, s *lc.Server, h *health, counter *AttemptCounter, bare bool) {
	var opts []grpc.ServerOption
	if !bare {
		opts = append(opts, grpc.UnaryInterceptor(UnaryServerInterceptor(s, counter)),
			grpc.StreamInterceptor(StreamServerInterceptor(s, counter)))
	}
	srv := grpc.NewServer(opts...)
	grpc_health_v1.RegisterHealthServer(srv, h)
	go srv.Serve(lis)
	tb.Cleanup(func() {
		srv.Stop()
		if s != nil {
			s.Close()
		}
	})
}

// dial connects to lis, through the client interceptors unless bare (c may
// be nil for priority propagation only).
func dial(tb testing.TB, lis net.Listener, c *lc.Client, bare bool) grpc_health_v1.HealthClient {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	target := "passthrough:///" + lis.Addr().String()
	if bl, ok := lis.(*bufconn.Listener); ok {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return bl.DialContext(ctx) }))
		target = "passthrough:///bufnet"
	}
	if !bare {
		opts = append(opts, grpc.WithUnaryInterceptor(UnaryClientInterceptor(c)), grpc.WithStreamInterceptor(StreamClientInterceptor(c)))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })
	return grpc_health_v1.NewHealthClient(conn)
}

// node is one service in a test topology.
type node struct {
	h       *health
	counter *AttemptCounter
	lis     *bufconn.Listener
}

func newNode(t *testing.T, s *lc.Server, fn func(context.Context) error) *node {
	n := &node{h: &health{fn: fn}, counter: &AttemptCounter{}, lis: bufconn.Listen(1 << 20)}
	serve(t, n.lis, s, n.h, n.counter, false)
	return n
}

func (n *node) total() int64 { return n.counter.Original.Load() + n.counter.Retry.Load() }

var req = &grpc_health_v1.HealthCheckRequest{}

// Shedding: a full fixed limit rejects with RESOURCE_EXHAUSTED, the
// x-lc-shed trailer and the configured pushback.
func TestShedWhenFull(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	s := lc.NewServer(lc.ServerConfig{Limiter: limit.NewLimiter(&limit.Fixed{N: 1}, limit.Options{}), Pushback: 250 * time.Millisecond})
	n := newNode(t, s, func(context.Context) error { entered <- struct{}{}; <-release; return nil })
	cl := dial(t, n.lis, nil, false)
	done := make(chan error)
	go func() { _, err := cl.Check(context.Background(), req); done <- err }()
	<-entered

	var tr metadata.MD
	_, err := cl.Check(context.Background(), req, grpc.Trailer(&tr))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second call: %v, want ResourceExhausted", err)
	}
	if got := tr.Get(retry.ShedHeader); len(got) != 1 || got[0] != "limit" {
		t.Fatalf("shed trailer %v", got)
	}
	if got := tr.Get(retry.PushbackHeader); len(got) != 1 || got[0] != "250" {
		t.Fatalf("pushback trailer %v", got)
	}
	// Streams are admitted at their start and shed the same way.
	st, err := cl.Watch(context.Background(), req)
	if err == nil {
		_, err = st.Recv()
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("stream while full: %v", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first call: %v", err)
	}
	if n.h.calls.Load() != 1 || s.ShedLimit.Load() != 2 {
		t.Fatalf("handler calls %d, shed %d", n.h.calls.Load(), s.ShedLimit.Load())
	}
	if st, err = cl.Watch(context.Background(), req); err == nil {
		_, err = st.Recv()
	}
	if err != nil {
		t.Fatalf("stream with room: %v", err)
	}
}

// Deadline drop: a request that arrives with less than MinBudget left is
// refused with DEADLINE_EXCEEDED before the handler runs.
func TestDeadlineDrop(t *testing.T) {
	s := lc.NewServer(lc.ServerConfig{DropExpired: true, MinBudget: time.Second})
	n := newNode(t, s, nil)
	cl := dial(t, n.lis, nil, false)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var tr metadata.MD
	_, err := cl.Check(ctx, req, grpc.Trailer(&tr))
	if status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil {
		t.Fatalf("got %v (ctx %v), want a server-side DeadlineExceeded", err, ctx.Err())
	}
	if got := tr.Get(retry.ShedHeader); len(got) != 1 || got[0] != "deadline" {
		t.Fatalf("shed trailer %v", got)
	}
	if n.h.calls.Load() != 0 {
		t.Fatalf("handler ran %d times", n.h.calls.Load())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := cl.Check(ctx2, req); err != nil {
		t.Fatalf("ample deadline: %v", err)
	}
}

// Priority set by the original caller is seen two hops down.
func TestPriorityTwoHops(t *testing.T) {
	var seen atomic.Value
	b := newNode(t, lc.NewServer(lc.ServerConfig{}), func(ctx context.Context) error {
		p, _ := priority.FromContext(ctx)
		seen.Store(p)
		return nil
	})
	toB := dial(t, b.lis, nil, false)
	a := newNode(t, nil, func(ctx context.Context) error { _, err := toB.Check(ctx, req); return err })
	cl := dial(t, a.lis, nil, false)
	for _, c := range []struct {
		md   []string
		want priority.Info
	}{
		{[]string{priority.Header, "critical", priority.UserHeader, "42"}, priority.Info{Tier: priority.Critical, User: 42}},
		{[]string{priority.Header, "2", priority.UserHeader, "7"}, priority.Info{Tier: priority.Sheddable, User: 7}},
	} {
		ctx := metadata.AppendToOutgoingContext(context.Background(), c.md...)
		if _, err := cl.Check(ctx, req); err != nil {
			t.Fatal(err)
		}
		if got := seen.Load().(priority.Info); got != c.want {
			t.Fatalf("B saw %+v, want %+v", got, c.want)
		}
	}
	// A priority on the caller's context is sent as metadata too.
	if _, err := cl.Check(priority.WithInfo(context.Background(), priority.Info{Tier: priority.Critical, User: 9}), req); err != nil {
		t.Fatal(err)
	}
	if got := seen.Load().(priority.Info); got.Tier != priority.Critical || got.User != 9 {
		t.Fatalf("B saw %+v", got)
	}
}

// Retry amplification: client -> A -> B -> C with C always overloaded and
// 3 attempts at every hop. Naive retries multiply at each layer: A sees 3
// requests, B 3*3 = 9 and C 3*3*3 = 27 per top-level call. With the
// one-layer rule only B, directly in front of C, retries: 1, 1, 3.
func TestRetryAtOneLayer(t *testing.T) {
	for _, c := range []struct {
		oneLayer bool
		a, b, c  int64
	}{{false, 3, 9, 27}, {true, 1, 1, 3}} {
		client := func() *lc.Client {
			return lc.NewClient(lc.ClientConfig{Budget: retry.Unlimited{}, MaxAttempts: 3, HonorNoRetry: c.oneLayer})
		}
		srv := func() *lc.Server { return lc.NewServer(lc.ServerConfig{OneLayer: c.oneLayer}) }
		nc := newNode(t, srv(), func(context.Context) error { return status.Error(codes.ResourceExhausted, "c overloaded") })
		toC := dial(t, nc.lis, client(), false)
		nb := newNode(t, srv(), func(ctx context.Context) error { _, err := toC.Check(ctx, req); return err })
		toB := dial(t, nb.lis, client(), false)
		na := newNode(t, srv(), func(ctx context.Context) error { _, err := toB.Check(ctx, req); return err })
		cl := dial(t, na.lis, client(), false)

		var tr metadata.MD
		_, err := cl.Check(context.Background(), req, grpc.Trailer(&tr))
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("oneLayer=%v: %v", c.oneLayer, err)
		}
		if na.total() != c.a || nb.total() != c.b || nc.total() != c.c {
			t.Fatalf("oneLayer=%v: A %d B %d C %d, want %d %d %d", c.oneLayer, na.total(), nb.total(), nc.total(), c.a, c.b, c.c)
		}
		if c.oneLayer && len(tr.Get(retry.NoRetryHeader)) == 0 {
			t.Fatal("A's failure not marked no-retry")
		}
		if !c.oneLayer && (nc.counter.Original.Load() != 9 || nc.counter.Retry.Load() != 18) {
			t.Fatalf("C originals %d retries %d, want 9 and 18", nc.counter.Original.Load(), nc.counter.Retry.Load())
		}
	}
}

// A retry budget caps what one client adds against a dead backend. With 10
// tokens and every attempt failing: call 1 drains 10 to 7 with 2 retries,
// call 2 retries once (6 > 5), then the bucket sits at or below half.
func TestBudgetAcrossCalls(t *testing.T) {
	n := newNode(t, lc.NewServer(lc.ServerConfig{}), func(context.Context) error { return status.Error(codes.Unavailable, "down") })
	cl := dial(t, n.lis, lc.NewClient(lc.ClientConfig{Budget: retry.NewBudget(10, 0.1), MaxAttempts: 3}), false)
	for i := 0; i < 20; i++ {
		cl.Check(context.Background(), req)
	}
	if n.counter.Original.Load() != 20 || n.counter.Retry.Load() != 3 {
		t.Fatalf("originals %d retries %d, want 20 and 3", n.counter.Original.Load(), n.counter.Retry.Load())
	}
}

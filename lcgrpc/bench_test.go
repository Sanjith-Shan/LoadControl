package lcgrpc

import (
	"context"
	"net"
	"testing"
	"time"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/limit"
	"github.com/Sanjith-Shan/LoadControl/priority"
	"github.com/Sanjith-Shan/LoadControl/retry"
	"github.com/Sanjith-Shan/LoadControl/throttle"
	"google.golang.org/grpc/test/bufconn"
)

// Experiment exp7, middleware overhead: one unary call at a time over
// bufconn (no network) and loopback TCP, with no interceptors against the
// full stack (Gradient2 limiter with tier shares, DAGOR on the queue-wait
// signal, deadline drop, one-layer marking, client throttle and retry
// budget).

func listeners(b *testing.B) map[string]func() net.Listener {
	return map[string]func() net.Listener{
		"bufconn": func() net.Listener { return bufconn.Listen(1 << 20) },
		"tcp": func() net.Listener {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			return l
		},
	}
}

func fullServer() *lc.Server {
	return lc.NewServer(lc.ServerConfig{
		Name:        "bench",
		Limiter:     limit.NewLimiter(limit.NewGradient2(100), limit.Options{Shares: []float64{1, 0.9, 0.7}, MaxWait: 10 * time.Millisecond}),
		Dagor:       priority.NewDagor(nil),
		DagorSignal: "wait",
		DropExpired: true, MinBudget: time.Millisecond,
		OneLayer: true,
	})
}

func fullClient() *lc.Client {
	return lc.NewClient(lc.ClientConfig{
		Service: "bench", Target: "bench",
		Throttle:    throttle.New(2, 2*time.Minute, nil),
		Budget:      retry.NewBudget(10, 0.1),
		MaxAttempts: 3, HonorPushback: true, HonorNoRetry: true,
	})
}

func benchUnary(b *testing.B, bare bool) {
	for _, name := range []string{"bufconn", "tcp"} {
		b.Run(name, func(b *testing.B) {
			lis := listeners(b)[name]()
			var s *lc.Server
			var c *lc.Client
			if !bare {
				s, c = fullServer(), fullClient()
			}
			serve(b, lis, s, &health{}, nil, bare)
			cl := dial(b, lis, c, bare)
			ctx, cancel := context.WithTimeout(priority.WithInfo(context.Background(), priority.Info{Tier: priority.Default, User: 3}), time.Hour)
			defer cancel()
			if _, err := cl.Check(ctx, req); err != nil { // connect outside the timed loop
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := cl.Check(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnaryBare(b *testing.B)         { benchUnary(b, true) }
func BenchmarkUnaryInterceptors(b *testing.B) { benchUnary(b, false) }

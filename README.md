# LoadControl

LoadControl is overload control for Go microservices: gRPC-Go interceptors
and net/http middleware that keep a service doing useful work when it
receives more than it can handle. It sheds low-priority work first, drops
requests whose deadline has already passed, caps retries with a budget,
and makes sure only one layer of a call chain retries. It is measured on
DeathStarBench hotelReservation, a public Go and gRPC microservice
benchmark, running in Docker on one small machine. It does not claim
production results: every number here comes from containers on a single
4-core mini PC, and every algorithm in it is published (see
[DESIGN.md](DESIGN.md) for the sources).

Work in progress. Measured results land in [NUMBERS.md](NUMBERS.md).

## Pieces

| Piece | Where |
|---|---|
| Adaptive concurrency limits: AIMD, Vegas, Gradient2 | `limit/` |
| Priority tiers with per-tier shares of the limit, DAGOR admission level | `limit/`, `priority/` |
| Deadline propagation and dead-work dropping | root, `lcgrpc/`, `lchttp/` |
| Client-side adaptive throttling (SRE book) | `throttle/` |
| Retry budget (gRFC A6 token bucket), server pushback, retry at one layer | `retry/`, root |
| gRPC unary and stream interceptors, HTTP middleware and RoundTripper | `lcgrpc/`, `lchttp/` |
| Prometheus metrics and a Grafana dashboard | `metrics.go`, `bench/grafana/` |
| Open-loop load generator with tiers and user retries | `cmd/loadgen/` |
| Discrete-event simulator that runs the library's own policy code | `sim/`, `cmd/lcsim/` |

## Use

```go
srv := loadcontrol.NewServer(loadcontrol.ServerConfig{
    Name:        "search",
    Limiter:     limit.NewLimiter(limit.NewGradient2(20), limit.Options{Shares: []float64{1, 0.9, 0.7}}),
    DropExpired: true,
    OneLayer:    true,
})
s := grpc.NewServer(grpc.ChainUnaryInterceptor(lcgrpc.UnaryServerInterceptor(srv, nil)))

cli := loadcontrol.NewClient(loadcontrol.ClientConfig{
    Target: "geo", Budget: retry.NewBudget(10, 0.1), MaxAttempts: 3,
    Throttle: throttle.New(2, 2*time.Minute, nil), HonorPushback: true, HonorNoRetry: true,
})
conn, _ := grpc.NewClient(addr, grpc.WithChainUnaryInterceptor(lcgrpc.UnaryClientInterceptor(cli)))
```

Or configure everything from `LC_*` environment variables with
`loadcontrol.FromEnv(name)`, which is what the benchmark does.

## Tests

`go test ./...` runs unit, property (pgregory.net/rapid) and integration
tests (gRPC over bufconn, HTTP over httptest, goroutine leak checks with
goleak). `go test -race ./...` needs cgo.

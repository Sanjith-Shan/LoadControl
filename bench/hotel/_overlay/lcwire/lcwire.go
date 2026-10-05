// Package lcwire connects the hotelReservation services to LoadControl.
// It is added to the benchmark by bench/hotel/apply_patch.sh. Every
// setting comes from LC_* environment variables (see loadcontrol.Env), so
// one image serves every experiment configuration.
package lcwire

import (
	"net/http"
	"os"
	"strings"
	"sync"

	lc "github.com/Sanjith-Shan/LoadControl"
	"github.com/Sanjith-Shan/LoadControl/lcgrpc"
	"github.com/Sanjith-Shan/LoadControl/lchttp"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

var (
	once    sync.Once
	service string
	server  *lc.Server
	metrics *lc.Metrics
)

// Init records which service this process runs and starts the metrics
// listener on :9100.
func Init(name string) {
	once.Do(func() {
		service = name
		metrics = lc.DefaultMetrics()
		env := lc.FromEnv(name)
		cfg, err := env.ServerConfig(metrics)
		if err != nil {
			log.Fatal().Msgf("loadcontrol config: %v", err)
		}
		if cfg.Limiter != nil || cfg.Dagor != nil || cfg.DropExpired || cfg.RateLimit != nil || cfg.DefaultTimeout > 0 || cfg.OneLayer {
			server = lc.NewServer(cfg)
		} else {
			// No admission pieces: still count inbound requests and
			// propagate priority, so the baseline is measured the same way.
			server = lc.NewServer(lc.ServerConfig{Name: name, Metrics: metrics})
		}
		log.Info().Msgf("loadcontrol %s: %v", name, env.Describe())
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			_ = http.ListenAndServe(":9100", mux)
		}()
	})
}

// ServerOption returns the interceptor chain for a gRPC server, with
// tracing first so shed requests still show up in traces.
func ServerOption(tracing grpc.UnaryServerInterceptor) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(tracing, lcgrpc.UnaryServerInterceptor(server, nil))
}

// ClientInterceptor returns the client interceptor for calls from this
// service to target (a consul URL or name).
func ClientInterceptor(target string) grpc.UnaryClientInterceptor {
	if i := strings.LastIndex(target, "/"); i >= 0 {
		target = target[i+1:]
	}
	cfg, err := lc.FromEnv(service).ClientConfig(target, metrics)
	if err != nil {
		log.Fatal().Msgf("loadcontrol client config: %v", err)
	}
	return lcgrpc.UnaryClientInterceptor(lc.NewClient(cfg))
}

// HTTP wraps the frontend's handler.
func HTTP(h http.Handler) http.Handler { return lchttp.Middleware(server, h) }

func init() {
	if v := os.Getenv("LC_SERVICE"); v != "" {
		Init(v)
	}
}

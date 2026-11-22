package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/version"
)

// Config holds the settings needed to start a broker process.
type Config struct {
	NodeID      string
	GRPCAddr    string // client-facing gRPC listen address
	MetricsAddr string // HTTP listen address for /metrics, /healthz, /readyz
	Logger      *slog.Logger
}

const shutdownTimeout = 10 * time.Second

// Run starts the broker and blocks until ctx is cancelled or a listener fails.
func Run(ctx context.Context, cfg Config) error {
	if cfg.NodeID == "" {
		return errors.New("broker: node ID is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("broker: listen gRPC: %w", err)
	}
	httpLis, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		grpcLis.Close()
		return fmt.Errorf("broker: listen metrics: %w", err)
	}

	grpcSrv, healthSrv := NewGRPCServer(cfg.NodeID, grpcLis.Addr().String())
	httpSrv := &http.Server{
		Handler:           newHTTPHandler(cfg.NodeID),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("broker starting",
		"node_id", cfg.NodeID,
		"version", version.Version,
		"grpc_addr", grpcLis.Addr().String(),
		"metrics_addr", httpLis.Addr().String())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return grpcSrv.Serve(grpcLis) })
	g.Go(func() error {
		if err := httpSrv.Serve(httpLis); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("broker shutting down")
		healthSrv.Shutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		stopped := make(chan struct{})
		go func() { grpcSrv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			grpcSrv.Stop()
		}
		return httpSrv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}

// NewGRPCServer builds the broker's gRPC server with all services registered.
// advertisedAddr is the address reported to clients in cluster status.
func NewGRPCServer(nodeID, advertisedAddr string) (*grpc.Server, *health.Server) {
	srv := grpc.NewServer()
	riftv1.RegisterClusterServiceServer(srv, &clusterService{nodeID: nodeID, address: advertisedAddr})

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(riftv1.ClusterService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	reflection.Register(srv)
	return srv, healthSrv
}

func newHTTPHandler(nodeID string) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "rift_build_info",
			Help:        "Build information for the running broker; always 1.",
			ConstLabels: prometheus.Labels{"version": version.Version, "node_id": nodeID},
		}, func() float64 { return 1 }),
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) }
	mux.HandleFunc("GET /healthz", ok)
	// Readiness will depend on storage recovery and cluster membership once those exist.
	mux.HandleFunc("GET /readyz", ok)
	return mux
}

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // accept gzip-compressed requests
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/offsets"
	"github.com/jayyy255/rift/internal/topics"
	"github.com/jayyy255/rift/internal/version"
)

// Config holds the settings needed to start a broker. Zero values select the
// defaults documented on each field.
type Config struct {
	NodeID      string
	GRPCAddr    string // client-facing gRPC listen address
	MetricsAddr string // HTTP listen address for /metrics, /healthz, /readyz
	DataDir     string

	// Storage layout; see logstore.Options.
	Log logstore.Options
	// Broker-wide retention defaults, overridable per topic. 0 = unlimited.
	RetentionMaxAge        time.Duration
	RetentionMaxBytes      int64
	RetentionCheckInterval time.Duration // default 1m
	// FlushInterval bounds how long DURABILITY_WRITTEN records stay unsynced.
	FlushInterval time.Duration // default 1s

	MaxRecordBytes int           // largest accepted record; default 1 MiB
	MaxBatchBytes  int           // largest publish request; default 16 MiB
	MaxFetchBytes  int           // cap on fetch response size; default 8 MiB
	MaxFetchWait   time.Duration // cap on fetch long-poll wait; default 30s

	// Disk watermarks as used/total ratios. Above high the broker warns;
	// above critical it rejects publishes until usage drops.
	DiskHighWatermark     float64       // default 0.85
	DiskCriticalWatermark float64       // default 0.95
	DiskCheckInterval     time.Duration // default 10s
	// DiskUsage reports filesystem usage for the data directory; tests
	// replace it. Defaults to statfs.
	DiskUsage func(path string) (DiskStats, error)

	Logger *slog.Logger
}

const (
	defaultMaxRecordBytes = 1 << 20
	defaultMaxBatchBytes  = 16 << 20
	defaultMaxFetchBytes  = 8 << 20
	defaultMaxFetchWait   = 30 * time.Second
	maxBatchRecords       = 10000
	shutdownTimeout       = 10 * time.Second
)

func (c *Config) applyDefaults() error {
	if c.NodeID == "" {
		return errors.New("broker: node ID is required")
	}
	if c.DataDir == "" {
		return errors.New("broker: data directory is required")
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = defaultMaxRecordBytes
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = defaultMaxBatchBytes
	}
	if c.MaxFetchBytes == 0 {
		c.MaxFetchBytes = defaultMaxFetchBytes
	}
	if c.MaxFetchWait == 0 {
		c.MaxFetchWait = defaultMaxFetchWait
	}
	if c.DiskHighWatermark == 0 {
		c.DiskHighWatermark = 0.85
	}
	if c.DiskCriticalWatermark == 0 {
		c.DiskCriticalWatermark = 0.95
	}
	if c.DiskCheckInterval == 0 {
		c.DiskCheckInterval = 10 * time.Second
	}
	if c.DiskUsage == nil {
		c.DiskUsage = statDisk
	}
	if c.MaxRecordBytes <= 0 || c.MaxBatchBytes < c.MaxRecordBytes {
		return errors.New("broker: max batch bytes must be at least max record bytes")
	}
	if !(0 < c.DiskHighWatermark && c.DiskHighWatermark <= c.DiskCriticalWatermark && c.DiskCriticalWatermark <= 1) {
		return errors.New("broker: disk watermarks must satisfy 0 < high <= critical <= 1")
	}
	return nil
}

// Broker is a single-node Rift broker: storage, offsets, and gRPC services.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	topics  *topics.Registry
	offsets *offsets.Store
	metrics *metrics
	disk    *diskMonitor

	grpc   *grpc.Server
	health *health.Server
	addr   atomic.Value // string: advertised client address
	ready  atomic.Bool

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// New opens the broker's storage, recovering every partition, and builds the
// gRPC server. Call Serve to accept connections and Close to shut down.
func New(cfg Config) (*Broker, error) {
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, log: cfg.Logger, stop: make(chan struct{})}
	b.addr.Store(cfg.GRPCAddr)
	b.metrics = newMetrics(b)
	b.disk = newDiskMonitor(cfg, b.metrics)

	reg, err := topics.Open(topics.Config{
		DataDir:                cfg.DataDir,
		Log:                    cfg.Log,
		RetentionMaxAge:        cfg.RetentionMaxAge,
		RetentionMaxBytes:      cfg.RetentionMaxBytes,
		RetentionCheckInterval: cfg.RetentionCheckInterval,
		FlushInterval:          cfg.FlushInterval,
		Logger:                 cfg.Logger,
		OnRetention:            b.metrics.observeRetention,
	})
	if err != nil {
		return nil, fmt.Errorf("broker: open storage: %w", err)
	}
	b.topics = reg

	if b.offsets, err = offsets.Open(filepath.Join(cfg.DataDir, "groups")); err != nil {
		reg.Close()
		return nil, fmt.Errorf("broker: open offsets: %w", err)
	}
	if err := b.reconcileRecovery(); err != nil {
		reg.Close()
		return nil, err
	}

	b.disk.check()
	b.wg.Go(func() { b.disk.run(b.stop) })

	b.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(cfg.MaxBatchBytes + 1<<20))
	riftv1.RegisterClusterServiceServer(b.grpc, &clusterService{b: b})
	riftv1.RegisterTopicServiceServer(b.grpc, &topicService{b: b})
	riftv1.RegisterProducerServiceServer(b.grpc, &producerService{b: b})
	riftv1.RegisterConsumerServiceServer(b.grpc, &consumerService{b: b})
	b.health = health.NewServer()
	healthpb.RegisterHealthServer(b.grpc, b.health)
	reflection.Register(b.grpc)
	return b, nil
}

// reconcileRecovery records recovery metrics and clamps consumer offsets that
// point past a partition's recovered end.
func (b *Broker) reconcileRecovery() error {
	for _, rec := range b.topics.Recoveries() {
		b.metrics.observeRecovery(rec.Report)
		changed, err := b.offsets.Clamp(rec.Topic, rec.Partition, rec.Report.EndOffset)
		if err != nil {
			return fmt.Errorf("broker: clamp offsets: %w", err)
		}
		for _, g := range changed {
			b.metrics.offsetsClamped.Inc()
			b.log.Warn("consumer offset was beyond recovered log end; clamped",
				"group", g, "topic", rec.Topic, "partition", rec.Partition, "end_offset", rec.Report.EndOffset)
		}
	}
	return nil
}

// Serve accepts gRPC connections on lis until Close is called.
func (b *Broker) Serve(lis net.Listener) error {
	b.addr.Store(lis.Addr().String())
	b.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	b.ready.Store(true)
	return b.grpc.Serve(lis)
}

// Close stops serving, then fsyncs and closes all storage. It is idempotent.
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.ready.Store(false)
		b.health.Shutdown()
		stopped := make(chan struct{})
		go func() { b.grpc.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(shutdownTimeout):
			b.grpc.Stop()
		}
		close(b.stop)
		b.wg.Wait()
		b.closeErr = b.topics.Close()
	})
	return b.closeErr
}

// Run starts a broker with cfg and blocks until ctx is cancelled or a
// listener fails, then shuts down gracefully.
func Run(ctx context.Context, cfg Config) error {
	b, err := New(cfg)
	if err != nil {
		return err
	}
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		b.Close()
		return fmt.Errorf("broker: listen gRPC: %w", err)
	}
	httpLis, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		grpcLis.Close()
		b.Close()
		return fmt.Errorf("broker: listen metrics: %w", err)
	}
	httpSrv := &http.Server{Handler: b.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}

	b.log.Info("broker started",
		"node_id", cfg.NodeID, "version", version.Version, "data_dir", cfg.DataDir,
		"grpc_addr", grpcLis.Addr().String(), "metrics_addr", httpLis.Addr().String())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Serve(grpcLis) })
	g.Go(func() error {
		if err := httpSrv.Serve(httpLis); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		b.log.Info("broker shutting down")
		cerr := b.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return errors.Join(cerr, httpSrv.Shutdown(shutdownCtx))
	})
	err = g.Wait()
	b.log.Info("broker stopped")
	return err
}

// HTTPHandler serves /metrics, /healthz (liveness) and /readyz (ready to
// serve clients: storage recovered and the gRPC server accepting).
func (b *Broker) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", b.metrics.handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !b.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// Command rift-broker is the Rift broker entry point.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jayyy255/rift/internal/broker"
	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/version"
)

func main() {
	var cfg broker.Config
	flags := flag.NewFlagSet("rift-broker", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print build version and exit")
	logLevel := flags.String("log-level", "info", "log level: debug, info, warn, error")

	flags.StringVar(&cfg.NodeID, "node-id", "rift-1", "unique broker identity")
	flags.StringVar(&cfg.GRPCAddr, "listen", "127.0.0.1:7070", "client gRPC listen address")
	flags.StringVar(&cfg.MetricsAddr, "metrics-listen", "127.0.0.1:9100", "HTTP listen address for /metrics, /healthz and /readyz")
	flags.StringVar(&cfg.DataDir, "data-dir", "./data", "directory for topic logs and consumer offsets")

	flags.Int64Var(&cfg.Log.SegmentBytes, "segment-bytes", logstore.DefaultSegmentBytes, "roll the active segment at this size (1 KiB - 1 GiB)")
	flags.DurationVar(&cfg.Log.SegmentMaxAge, "segment-max-age", 0, "roll a segment once its first record is this old, so time retention can delete it (0 = max of retention or 24h)")
	flags.Int64Var(&cfg.Log.IndexIntervalBytes, "index-interval-bytes", logstore.DefaultIndexIntervalBytes, "bytes between sparse offset-index entries")

	flags.DurationVar(&cfg.RetentionMaxAge, "retention", 0, "default time retention per partition (0 = unlimited)")
	flags.Int64Var(&cfg.RetentionMaxBytes, "retention-bytes", 0, "default size retention per partition (0 = unlimited)")
	flags.DurationVar(&cfg.RetentionCheckInterval, "retention-check-interval", 0, "how often retention runs (default 1m)")
	flags.DurationVar(&cfg.FlushInterval, "flush-interval", 0, "max time DURABILITY_WRITTEN records stay unsynced (default 1s)")

	flags.IntVar(&cfg.MaxRecordBytes, "max-record-bytes", 0, "largest accepted record (default 1 MiB)")
	flags.IntVar(&cfg.MaxBatchBytes, "max-batch-bytes", 0, "largest accepted publish request (default 16 MiB)")
	flags.Float64Var(&cfg.DiskHighWatermark, "disk-high-watermark", 0, "warn above this used/total disk ratio (default 0.85)")
	flags.Float64Var(&cfg.DiskCriticalWatermark, "disk-critical-watermark", 0, "reject publishes above this used/total disk ratio (default 0.95)")

	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "rift-broker - Rift single-node broker")
		fmt.Fprintln(flags.Output(), "Usage: rift-broker [flags]")
		flags.PrintDefaults()
	}
	_ = flags.Parse(os.Args[1:])
	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "rift-broker: unexpected argument %q\n", flags.Arg(0))
		flags.Usage()
		os.Exit(2)
	}
	if *showVersion {
		fmt.Printf("rift-broker %s\n", version.Version)
		return
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "rift-broker: invalid --log-level %q\n", *logLevel)
		os.Exit(2)
	}
	cfg.Logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if cfg.Log.SegmentMaxAge == 0 {
		// Time retention can only delete whole sealed segments, so segments
		// must roll at least as often as the retention period.
		cfg.Log.SegmentMaxAge = 24 * time.Hour
		if cfg.RetentionMaxAge > 0 && cfg.RetentionMaxAge < cfg.Log.SegmentMaxAge {
			cfg.Log.SegmentMaxAge = cfg.RetentionMaxAge
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := broker.Run(ctx, cfg); err != nil {
		cfg.Logger.Error("broker exited", "error", err)
		os.Exit(1)
	}
}

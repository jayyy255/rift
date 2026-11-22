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

	"github.com/jayyy255/rift/internal/broker"
	"github.com/jayyy255/rift/internal/version"
)

func main() {
	flags := flag.NewFlagSet("rift-broker", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print build version and exit")
	nodeID := flags.String("node-id", "rift-1", "unique broker identity")
	grpcAddr := flags.String("listen", "127.0.0.1:7070", "client gRPC listen address")
	metricsAddr := flags.String("metrics-listen", "127.0.0.1:9100", "HTTP listen address for /metrics, /healthz and /readyz")
	logLevel := flags.String("log-level", "info", "log level: debug, info, warn, error")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "rift-broker - Rift broker")
		fmt.Fprintln(flags.Output(), "Usage: rift-broker [flags]")
		flags.PrintDefaults()
		fmt.Fprintln(flags.Output(), "Only cluster diagnostics are served; messaging is not implemented yet.")
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
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := broker.Run(ctx, broker.Config{
		NodeID:      *nodeID,
		GRPCAddr:    *grpcAddr,
		MetricsAddr: *metricsAddr,
		Logger:      logger,
	})
	if err != nil {
		logger.Error("broker exited", "error", err)
		os.Exit(1)
	}
}

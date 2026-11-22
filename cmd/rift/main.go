// Command rift is the Rift administration CLI entry point.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/jayyy255/rift/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := cli.NewRootCommand().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "rift:", err)
		os.Exit(1)
	}
}

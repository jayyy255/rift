// Package cli implements the rift administration command tree.
package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/jayyy255/rift/internal/version"
	"github.com/jayyy255/rift/pkg/client"
)

type globalOptions struct {
	broker   string
	timeout  time.Duration
	compress bool
}

func (o *globalOptions) dial() (*client.Client, error) {
	opts := []client.Option{client.WithTimeout(o.timeout)}
	if o.compress {
		opts = append(opts, client.WithCompression())
	}
	c, err := client.Dial(o.broker, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", o.broker, err)
	}
	return c, nil
}

// NewRootCommand returns the rift command tree.
func NewRootCommand() *cobra.Command {
	opts := &globalOptions{}
	root := &cobra.Command{
		Use:           "rift",
		Short:         "Rift administration CLI",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetVersionTemplate("rift {{.Version}}\n")
	root.PersistentFlags().StringVar(&opts.broker, "broker", "127.0.0.1:7070", "broker gRPC address")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 10*time.Second, "per-request timeout")
	root.PersistentFlags().BoolVar(&opts.compress, "compress", false, "gzip-compress requests and responses")

	root.AddCommand(
		newClusterCommand(opts),
		newTopicCommand(opts),
		newPublishCommand(opts),
		newConsumeCommand(opts),
		newPartitionCommand(),
		newBenchmarkCommand(),
	)
	return root
}

// notImplemented marks commands whose broker support belongs to a later milestone.
func notImplemented(milestone string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		return fmt.Errorf("%q is not implemented yet (planned for milestone %s; see docs/roadmap.md)", cmd.CommandPath(), milestone)
	}
}

// Package cli implements the rift administration command tree.
package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/jayyy255/rift/internal/version"
)

type globalOptions struct {
	broker  string
	timeout time.Duration
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
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 5*time.Second, "per-request timeout")

	root.AddCommand(
		newClusterCommand(opts),
		newTopicCommand(),
		newPublishCommand(),
		newConsumeCommand(),
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

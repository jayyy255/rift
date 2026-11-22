package cli

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newClusterCommand(opts *globalOptions) *cobra.Command {
	cluster := &cobra.Command{Use: "cluster", Short: "Inspect cluster membership and health"}
	cluster.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show the brokers known to the target broker",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			node, brokers, err := c.ClusterStatus(cmd.Context())
			if err != nil {
				return fmt.Errorf("cluster status from %s: %w", opts.broker, err)
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Answered by %s\n\n", node)
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NODE\tADDRESS\tSTATE")
			for _, b := range brokers {
				fmt.Fprintf(tw, "%s\t%s\t%s\n", b.NodeID, b.Address, b.State)
			}
			return tw.Flush()
		},
	})
	return cluster
}

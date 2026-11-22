package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
)

func newClusterCommand(opts *globalOptions) *cobra.Command {
	cluster := &cobra.Command{Use: "cluster", Short: "Inspect cluster membership and health"}
	cluster.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show the brokers known to the target broker",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			conn, err := grpc.NewClient(opts.broker, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("connect to %s: %w", opts.broker, err)
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
			defer cancel()
			resp, err := riftv1.NewClusterServiceClient(conn).GetStatus(ctx, &riftv1.GetStatusRequest{})
			if err != nil {
				return fmt.Errorf("cluster status from %s: %w", opts.broker, err)
			}
			return printStatus(cmd, resp)
		},
	})
	return cluster
}

func printStatus(cmd *cobra.Command, resp *riftv1.GetStatusResponse) error {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Answered by %s (version %s)\n\n", resp.GetNodeId(), resp.GetVersion())
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tADDRESS\tSTATE")
	for _, b := range resp.GetBrokers() {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", b.GetNodeId(), b.GetAddress(), b.GetState())
	}
	return tw.Flush()
}

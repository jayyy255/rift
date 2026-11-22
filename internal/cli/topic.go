package cli

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/jayyy255/rift/pkg/client"
)

func newTopicCommand(opts *globalOptions) *cobra.Command {
	topic := &cobra.Command{Use: "topic", Short: "Administer topics"}

	var cfg client.TopicConfig
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			info, err := c.CreateTopic(cmd.Context(), args[0], cfg)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Created topic %q with %d partition(s), replication factor %d\n",
				info.Name, info.Partitions, info.ReplicationFactor)
			return nil
		},
	}
	create.Flags().Int32Var(&cfg.Partitions, "partitions", 1, "number of partitions")
	create.Flags().Int32Var(&cfg.ReplicationFactor, "replicas", 1, "replication factor (only 1 until milestone R3)")
	create.Flags().DurationVar(&cfg.Retention, "retention", 0, "time retention per partition (0 = broker default, negative = unlimited)")
	create.Flags().Int64Var(&cfg.RetentionBytes, "retention-bytes", 0, "size retention per partition (0 = broker default, negative = unlimited)")

	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a topic and all of its data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			if err := c.DeleteTopic(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted topic %q\n", args[0])
			return nil
		},
	}

	list := &cobra.Command{
		Use:   "list",
		Short: "List topics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			topics, err := c.ListTopics(cmd.Context())
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tREPLICAS\tRETENTION\tRETENTION BYTES")
			for _, t := range topics {
				fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n", t.Name, t.Partitions, t.ReplicationFactor,
					retentionString(t.RetentionMs, func(v int64) string { return (time.Duration(v) * time.Millisecond).String() }),
					retentionString(t.RetentionBytes, func(v int64) string { return fmt.Sprint(v) }))
			}
			return tw.Flush()
		},
	}

	describe := &cobra.Command{
		Use:   "describe <name>",
		Short: "Show a topic's partitions, offsets, and sizes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			info, parts, err := c.DescribeTopic(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Topic %s: %d partition(s), replication factor %d, created %s\n\n",
				info.Name, info.Partitions, info.ReplicationFactor, info.CreatedAt.UTC().Format(time.RFC3339))
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "PARTITION\tLEADER\tSTART\tEND\tSYNCED\tSEGMENTS\tSIZE")
			for _, p := range parts {
				fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%d\t%d\t%d\n", p.Partition, p.Leader, p.LogStartOffset,
					p.HighWatermark, p.SyncedOffset, p.Segments, p.SizeBytes)
			}
			return tw.Flush()
		},
	}

	topic.AddCommand(create, del, list, describe)
	return topic
}

func retentionString(v int64, format func(int64) string) string {
	switch {
	case v == 0:
		return "default"
	case v < 0:
		return "unlimited"
	default:
		return format(v)
	}
}

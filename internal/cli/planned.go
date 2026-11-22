package cli

import "github.com/spf13/cobra"

// Commands below define the target CLI surface from the specification. Their
// flags are fixed now so the UX can be reviewed; the RPCs arrive per milestone.

func newTopicCommand() *cobra.Command {
	topic := &cobra.Command{Use: "topic", Short: "Administer topics"}
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented("R1"),
	}
	create.Flags().Int("partitions", 1, "number of partitions")
	create.Flags().Int("replicas", 1, "replication factor")
	topic.AddCommand(create)
	return topic
}

func newPublishCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish <topic>",
		Short: "Publish one record",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented("R1"),
	}
	cmd.Flags().String("key", "", "record key; records with the same key keep their order")
	cmd.Flags().String("data", "", "record payload")
	return cmd
}

func newConsumeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume records as a stream consumer or queue worker",
		Args:  cobra.ExactArgs(1),
		RunE:  notImplemented("R4 (stream) / R5 (queue)"),
	}
	cmd.Flags().String("group", "", "consumer group")
	cmd.Flags().String("mode", "stream", "consumption mode: stream or queue")
	return cmd
}

func newPartitionCommand() *cobra.Command {
	partition := &cobra.Command{Use: "partition", Short: "Inspect and move partitions"}
	rebalance := &cobra.Command{
		Use:   "rebalance",
		Short: "Plan or apply partition placement changes",
		Args:  cobra.NoArgs,
		RunE:  notImplemented("R6"),
	}
	rebalance.Flags().Bool("dry-run", false, "print the plan without applying it")
	partition.AddCommand(rebalance)
	return partition
}

func newBenchmarkCommand() *cobra.Command {
	bench := &cobra.Command{Use: "benchmark", Short: "Run reproducible workload benchmarks"}
	run := &cobra.Command{
		Use:   "run",
		Short: "Run a benchmark profile",
		Args:  cobra.NoArgs,
		RunE:  notImplemented("R6"),
	}
	run.Flags().String("profile", "uniform", "workload profile, e.g. uniform or skewed-keys")
	bench.AddCommand(run)
	return bench
}

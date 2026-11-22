package cli

import "github.com/spf13/cobra"

// Commands below are part of the target CLI surface from the specification;
// their RPCs arrive in later milestones.

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

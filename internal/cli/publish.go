package cli

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/jayyy255/rift/pkg/client"
)

func newPublishCommand(opts *globalOptions) *cobra.Command {
	var (
		key, data, durability string
		partition             int32
		headers               []string
		fromStdin             bool
	)
	cmd := &cobra.Command{
		Use:   "publish <topic>",
		Short: "Publish records",
		Long: "Publish one record from --data, or one record per input line with --stdin.\n" +
			"Records with the same --key go to the same partition and keep their order.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := parseDurability(durability)
			if err != nil {
				return err
			}
			if fromStdin == cmd.Flags().Changed("data") {
				return fmt.Errorf("provide exactly one of --data or --stdin")
			}
			base := client.Record{}
			if cmd.Flags().Changed("key") {
				base.Key = []byte(key)
			}
			if partition >= 0 {
				base.Partition = &partition
			}
			for _, h := range headers {
				k, v, ok := strings.Cut(h, "=")
				if !ok {
					return fmt.Errorf("header %q must be key=value", h)
				}
				base.Headers = append(base.Headers, client.Header{Key: k, Value: []byte(v)})
			}

			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			out := cmd.OutOrStdout()

			if !fromStdin {
				rec := base
				rec.Value = []byte(data)
				mds, err := c.Publish(cmd.Context(), args[0], dur, rec)
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "Published to %s partition %d at offset %d\n", args[0], mds[0].Partition, mds[0].Offset)
				return nil
			}

			p := c.NewProducer(args[0], client.ProducerConfig{Durability: dur})
			var (
				count    int
				firstErr error
			)
			results := make(chan error, 1024)
			done := make(chan struct{})
			go func() {
				for err := range results {
					if err != nil && firstErr == nil {
						firstErr = err
					}
				}
				close(done)
			}()
			sc := bufio.NewScanner(cmd.InOrStdin())
			sc.Buffer(make([]byte, 64<<10), 16<<20)
			for sc.Scan() {
				rec := base
				rec.Value = []byte(sc.Text())
				if err := p.SendAsync(cmd.Context(), rec, func(_ client.RecordMetadata, err error) { results <- err }); err != nil {
					p.Close()
					return err
				}
				count++
			}
			p.Close()
			close(results)
			<-done
			if err := sc.Err(); err != nil {
				return err
			}
			if firstErr != nil {
				return firstErr
			}
			fmt.Fprintf(out, "Published %d record(s) to %s\n", count, args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&key, "key", "", "record key (omit for keyless round-robin routing)")
	cmd.Flags().StringVar(&data, "data", "", "record value")
	cmd.Flags().BoolVar(&fromStdin, "stdin", false, "publish one record per line of standard input")
	cmd.Flags().Int32Var(&partition, "partition", -1, "explicit partition (default: route by key, or round-robin)")
	cmd.Flags().StringArrayVar(&headers, "header", nil, "record header as key=value (repeatable)")
	cmd.Flags().StringVar(&durability, "durability", "fsynced", "acknowledgment level: fsynced or written")
	return cmd
}

func parseDurability(s string) (client.Durability, error) {
	switch s {
	case "fsynced":
		return client.DurabilityFsynced, nil
	case "written":
		return client.DurabilityWritten, nil
	default:
		return 0, fmt.Errorf("unknown durability %q (use fsynced or written)", s)
	}
}

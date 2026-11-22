package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jayyy255/rift/pkg/client"
)

func newConsumeCommand(opts *globalOptions) *cobra.Command {
	var (
		group, mode, from, output string
		maxMessages               int
		idleTimeout               time.Duration
	)
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume records as a stream consumer",
		Long: "Print records from every partition of a topic until interrupted, --max-messages\n" +
			"is reached, or no record arrives for --idle-timeout. With --group, consumption\n" +
			"resumes from the group's committed offsets and progress is committed after each\n" +
			"printed batch.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch mode {
			case "stream":
			case "queue":
				return notImplemented("R5")(cmd, args)
			default:
				return fmt.Errorf("unknown --mode %q (use stream or queue)", mode)
			}
			cfg := client.ConsumerConfig{Topic: args[0], Group: group}
			switch from {
			case "":
				// Committed with a group, earliest without.
			case "committed":
				if group == "" {
					return errors.New("--from committed requires --group")
				}
				cfg.Start = client.StartCommitted
			case "earliest":
				cfg.Start = client.StartEarliest
			case "latest":
				cfg.Start = client.StartLatest
			default:
				return fmt.Errorf("unknown --from %q (use committed, earliest, or latest)", from)
			}
			if output != "text" && output != "json" {
				return fmt.Errorf("unknown --output %q (use text or json)", output)
			}

			c, err := opts.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			ctx := cmd.Context()
			cons, err := c.NewConsumer(ctx, cfg)
			if err != nil {
				return err
			}
			return consumeLoop(ctx, cons, cmd.OutOrStdout(), group != "", output, maxMessages, idleTimeout)
		},
	}
	cmd.Flags().StringVar(&group, "group", "", "consumer group whose offsets are resumed and committed")
	cmd.Flags().StringVar(&mode, "mode", "stream", "consumption mode: stream (queue mode arrives in R5)")
	cmd.Flags().StringVar(&from, "from", "", "start position: committed, earliest, or latest (default committed with --group, else earliest)")
	cmd.Flags().IntVar(&maxMessages, "max-messages", 0, "stop after this many records (0 = no limit)")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0, "stop when no record arrives for this long (0 = wait forever)")
	cmd.Flags().StringVar(&output, "output", "text", "output format: text or json (one object per line)")
	return cmd
}

func consumeLoop(ctx context.Context, cons *client.Consumer, out io.Writer, commit bool, output string, maxMessages int, idle time.Duration) error {
	printed := 0
	lastRecord := time.Now()
	commitNow := func() error {
		if !commit {
			return nil
		}
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return cons.Commit(cctx)
	}
	for {
		msgs, err := cons.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				return commitNow() // interrupted: keep progress of printed records
			}
			return err
		}
		if len(msgs) == 0 {
			if idle > 0 && time.Since(lastRecord) >= idle {
				return commitNow()
			}
			continue
		}
		lastRecord = time.Now()
		for i, m := range msgs {
			if maxMessages > 0 && printed == maxMessages {
				// Rewind partitions to the first unprinted record so the
				// commit covers only what was printed.
				rewind := map[int32]int64{}
				for _, u := range msgs[i:] {
					if off, ok := rewind[u.Partition]; !ok || u.Offset < off {
						rewind[u.Partition] = u.Offset
					}
				}
				for p, off := range rewind {
					if err := cons.Seek(p, off); err != nil {
						return err
					}
				}
				break
			}
			if err := printMessage(out, m, output); err != nil {
				return err
			}
			printed++
		}
		if err := commitNow(); err != nil {
			return err
		}
		if maxMessages > 0 && printed >= maxMessages {
			return nil
		}
	}
}

func printMessage(out io.Writer, m client.Message, format string) error {
	if format == "json" {
		type header struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		obj := struct {
			Topic     string   `json:"topic"`
			Partition int32    `json:"partition"`
			Offset    int64    `json:"offset"`
			Timestamp string   `json:"timestamp"`
			Key       *string  `json:"key"`
			Value     string   `json:"value"`
			Headers   []header `json:"headers,omitempty"`
		}{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Timestamp: m.Timestamp.UTC().Format(time.RFC3339Nano), Value: string(m.Value)}
		if m.Key != nil {
			k := string(m.Key)
			obj.Key = &k
		}
		for _, h := range m.Headers {
			obj.Headers = append(obj.Headers, header{h.Key, string(h.Value)})
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(b))
		return err
	}
	if m.Key != nil {
		_, err := fmt.Fprintf(out, "[%s/%d@%d] key=%s %s\n", m.Topic, m.Partition, m.Offset, m.Key, m.Value)
		return err
	}
	_, err := fmt.Fprintf(out, "[%s/%d@%d] %s\n", m.Topic, m.Partition, m.Offset, m.Value)
	return err
}

# SDK examples

| Example | Shows | Delivery assumptions |
| --- | --- | --- |
| [quickstart](quickstart/main.go) | Create a topic, publish keyed records with a batching producer, consume with a group | `fsynced` acks; at-least-once consumption with auto-commit on Close |

Run a broker first (`make run`), then `go run ./examples/quickstart`.
Stream replay and queue-worker examples will follow with milestones R4 and R5.

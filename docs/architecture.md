# Architecture

## Design direction

Rift separates the control plane from the data plane. Raft will maintain metadata
such as membership, topics, and partition ownership. A separately specified
leader/follower replication protocol will carry partition data.

| Boundary | Intended responsibility |
| --- | --- |
| `api/proto` | Versioned gRPC/Protobuf transport contract |
| `internal/logstore` | Append-only WAL, checksums, segments, sparse index, recovery |
| `internal/cluster` | Raft metadata, membership, partition ownership |
| `internal/replication` | Leader epochs, fencing, replication, commit watermark |
| `internal/queue` | Persisted leases, per-record ACK/NACK, retry, DLQ |
| `internal/routing` | Key-to-partition routing and future placement controls |
| `pkg/client` | Typed producer, stream consumer, and queue worker SDK |
| `cmd/rift` | Topic administration, publishing, consumption, diagnostics |
| `cmd/rift-broker` | Broker process lifecycle and service composition |

## Paths to implement

Publish: route to leader, validate epoch/sequence, append, replicate according to
policy, advance commit watermark, then return the documented acknowledgment.

Stream: discover assignment, fetch from offset, process, and persist checkpoint.
Replay remains subject to retention.

Queue: persist claim and lease, process, then acknowledge the individual record.
Expired leases and NACKs trigger bounded retries; exhausted records go to a DLQ.

## Correctness requirements

- Ordering applies within a partition; future migrations must preserve per-key order.
- Acknowledgments must state their durability policy explicitly.
- Only a valid fenced partition leader may commit data.
- Initial delivery is at least once; consumers must handle duplicates.
- A consumer ACK and an external side effect are not inherently atomic.
- Buffers and retries must be bounded; overload must be observable.

## Current implementation (R1: single node)

| Package | Implemented |
| --- | --- |
| `internal/logstore` | Segmented WAL with CRC-32C frames, sparse index, rotation, group-commit fsync, crash recovery, retention, read-only inspection |
| `internal/topics` | Atomic topic create/delete, per-partition logs, key routing, background flush and retention, fetch wake-ups |
| `internal/offsets` | Durable consumer-group offsets, clamped after recovery |
| `internal/routing` | Frozen FNV-1a key-to-partition mapping |
| `internal/broker` | gRPC services (cluster, topic, producer, consumer, health), Prometheus metrics, disk watermarks, readiness |
| `pkg/client` | Go SDK: admin, batching producer with bounded buffer and retries, stream consumer with commits, replay, and reset policies |
| `cmd/rift` | `cluster status`, `topic create/delete/list/describe`, `publish`, `consume --mode stream` |

Not yet implemented: metadata consensus (R2), replication and leader epochs
(R3; epochs are reported as 0), group membership and assignment (R4), queue
mode (R5), adaptive placement (R6). See [storage-format.md](storage-format.md)
for the disk format, durability levels, recovery rules, and known limitations.

## Scope

Headless broker, Go SDK, CLI, and existing observability tools. A custom web UI,
universal exactly-once semantics, and Kafka wire compatibility are outside the MVP.

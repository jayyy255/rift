# Protocol definitions

Versioned Protobuf packages live under `rift/v1/`. Generated Go code is written to
`api/gen/` (package `riftv1`) and committed.

```sh
make proto        # buf generate (tools pinned in tools/go.mod)
make proto-check  # buf lint + fail if api/gen is stale
```

| Service | RPCs |
| --- | --- |
| `ClusterService` | `GetStatus` |
| `TopicService` | `CreateTopic`, `DeleteTopic`, `ListTopics`, `DescribeTopic` |
| `ProducerService` | `Publish` (explicit `Durability`: `WRITTEN` or `FSYNCED`) |
| `ConsumerService` | `Fetch` (multi-partition long poll), `CommitOffsets`, `GetCommittedOffsets` |

Errors use gRPC status codes: `UNAVAILABLE` and `RESOURCE_EXHAUSTED` are
retryable; `NOT_FOUND`, `INVALID_ARGUMENT`, `OUT_OF_RANGE`, and `DATA_LOSS` are
not. Per-partition fetch problems are reported in `PartitionRecords.error`.
Queue leases arrive with milestone R5.
Lint rules (`STANDARD`) and breaking-change rules (`FILE`) are in `/buf.yaml`.

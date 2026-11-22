# Implementation roadmap

Each stage requires its exit evidence before the next stage relies on its guarantees.

| Stage | Status | Deliverable | Exit evidence |
| --- | --- | --- | --- |
| R1: Single node | **Done** | WAL, protocol, SDK, offsets, retention | Crash/truncate at write boundaries; recover without invalid committed records |
| R2: Metadata | Pending | Three-node membership and Raft metadata | Kill the metadata leader; elect a valid replacement and recover |
| R3: Replication | Pending | Replicated logs, watermarks, epochs, fencing | Acknowledged records survive failures under the chosen policy |
| R4: Stream groups | Pending | Assignment, heartbeat, rebalance, checkpoints | Rebalance under load with exclusive ownership and measured duplicates |
| R5: Queue mode | Pending | Durable leases, ACK/NACK, retries, DLQ | Worker crashes cause correct redelivery within retry bounds |
| R6: Adaptation | Pending | Backpressure and hot-partition placement | Repeated baseline/candidate skew tests, including ordering checks |
| R7: Advanced | Pending | Security, tiering, secondary SDKs, expanded chaos | Published recovery, correctness, and performance results |

## R1 exit evidence

Run `make r1-acceptance`; JSON reports are written to `bin/reports/` (CI
uploads them as the `r1-acceptance-reports` artifact).

- `TestR1_CrashAtArbitraryWritePoint` (tests/chaos): a writer process appends
  and fsyncs; it is SIGKILLed at random moments across 25 chained iterations,
  and power loss is simulated after each kill by truncating, garbling, or
  zero-filling the unsynced tail. Every acknowledged record must survive,
  every recovered record must be contiguous and byte-identical, and appends
  must resume at the recovered end.
- `TestR1_BrokerCrashRecovery` (tests/integration): the real `rift-broker`
  is SIGKILLed while four producers publish FSYNCED records and a consumer
  group commits. After each restart every acknowledged record is at its
  acknowledged offset, partitions are contiguous, per-key order holds, and
  committed offsets do not exceed partition ends.
- `TestR1_GracefulShutdownKeepsWrittenRecords` and
  `TestR1_CLIAgainstBrokerProcess` cover SIGTERM flushing and the CLI.
- Supporting evidence: truncation at every byte of the active segment,
  every-bit-flip detection, a rapid model test with power-loss steps, and a
  frame-decoder fuzz target.

The format and contract are specified in [storage-format.md](storage-format.md)
and [ADR 0002](decisions/0002-r1-storage-and-durability.md).

## Next work items (R2)

1. Choose the metadata state machine contents (membership, topics, partition
   ownership) and wire HashiCorp Raft (ADR 0001).
2. Three-broker Docker Compose cluster with persistent volumes.
3. Exit test: kill the metadata leader; a valid new leader is elected and
   metadata recovers.

## Performance evidence

Benchmark uniform, skewed, burst, replay, task-worker, and failure workloads.
Record hardware, runtime, payload sizes, batching, compression, concurrency,
durability policy, replication factor, retention, and network settings. Report
throughput alongside P50/P95/P99 latency, lag, duplicates, and failures. Repeat
runs and quantify uncertainty before making comparative claims.

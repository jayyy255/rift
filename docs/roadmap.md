# Implementation roadmap

Each stage requires its exit evidence before the next stage relies on its guarantees.
All stages below are pending.

| Stage | Deliverable | Exit evidence |
| --- | --- | --- |
| R1: Single node | WAL, protocol, SDK, offsets, retention | Crash/truncate at write boundaries; recover without invalid committed records |
| R2: Metadata | Three-node membership and Raft metadata | Kill the metadata leader; elect a valid replacement and recover |
| R3: Replication | Replicated logs, watermarks, epochs, fencing | Acknowledged records survive failures under the chosen policy |
| R4: Stream groups | Assignment, heartbeat, rebalance, checkpoints | Rebalance under load with exclusive ownership and measured duplicates |
| R5: Queue mode | Durable leases, ACK/NACK, retries, DLQ | Worker crashes cause correct redelivery within retry bounds |
| R6: Adaptation | Backpressure and hot-partition placement | Repeated baseline/candidate skew tests, including ordering checks |
| R7: Advanced | Security, tiering, secondary SDKs, expanded chaos | Published recovery, correctness, and performance results |

## First work items

1. Specify the WAL record format, checksum, offset model, and fsync/ACK contract.
2. Implement segment append/read and sparse indexing.
3. Test partial writes, corruption, truncation, rotation, and restart recovery.
4. Define the first versioned Protobuf contract and reproducible generation tools.
5. Wire a single-node broker, SDK, and CLI to the verified logstore.

## Performance evidence

Benchmark uniform, skewed, burst, replay, task-worker, and failure workloads.
Record hardware, runtime, payload sizes, batching, compression, concurrency,
durability policy, replication factor, retention, and network settings. Report
throughput alongside P50/P95/P99 latency, lag, duplicates, and failures. Repeat
runs and quantify uncertainty before making comparative claims.

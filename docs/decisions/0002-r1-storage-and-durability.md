# 0002: R1 storage format, durability contract, and recovery policy

Status: accepted (2026-11-22)

## Context

Milestone R1 requires a single-node durable log whose exit test is: crash at
an arbitrary write point and restart without invalid committed records. The
specification requires that local and quorum acknowledgments are never
conflated and that delivery is at-least-once.

## Decisions

1. **Per-record frames with CRC-32C and a format byte.** Each record is
   independently verifiable, so recovery can stop at the exact first invalid
   byte and reads can detect bit rot. Batch-level framing (as in Kafka) would
   reduce overhead and allow batch compression, but makes partial-batch
   recovery coarser; it can be added later behind the format byte.
2. **Explicit durability levels per request** (`WRITTEN`, `FSYNCED`), with the
   achieved level echoed in the response. `FSYNCED` is the default:
   correctness before throughput. Group commit amortizes fsync cost.
3. **Asymmetric recovery.** The active segment's invalid tail is truncated and
   reported (it can only contain unacknowledged or torn writes). Sealed
   segments were fsynced at roll, so any damage there fails startup instead
   of silently dropping acknowledged data.
4. **Index files are derived and rebuildable.** They are not fsynced on every
   append; the log is the only source of truth.
5. **Sticky failure after a failed fsync or an un-undoable partial write.**
   The partition rejects writes until restart, where recovery re-establishes a
   valid prefix.
6. **Consumer offsets in per-group JSON files**, atomically replaced and
   fsynced per commit. Simple and crash-safe; a compacted internal log can
   replace it if commit rates demand it. Offsets above a recovered end are
   clamped at startup.
7. **Key routing with 32-bit FNV-1a modulo partitions**, frozen by golden
   tests, because changing it breaks per-key ordering across upgrades.
8. **Visibility at write time.** Consumers can read records before fsync. This
   keeps latency low; the hazard is documented in `docs/storage-format.md`. A
   later milestone can add a "read committed" fetch bounded by the synced
   offset (or, with replication, the high watermark).

## Consequences

- The exit test is automated as `TestR1_CrashAtArbitraryWritePoint` (process
  kill plus simulated power loss: truncation, garbage tails, zero-filled holes)
  and `TestR1_BrokerCrashRecovery` (SIGKILL of the real broker under load).
- `pgregory.net/rapid` moved into `go.mod` for the logstore model test.

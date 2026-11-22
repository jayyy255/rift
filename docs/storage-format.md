# Storage format and durability contract (R1)

This document specifies what the single-node broker writes to disk, how it
recovers after a crash, and exactly what each acknowledgment guarantees.

## Data directory layout

```text
<data-dir>/
  topics/
    <topic>/
      topic.json                  topic metadata (atomically replaced)
      <partition>/                one directory per partition: 0, 1, ...
        00000000000000000000.log  segment: records from base offset 0
        00000000000000000000.index
        00000000000000012345.log  next segment, base offset 12345
        00000000000000012345.index
    .creating-<topic>-*           topic being created (removed at startup)
    .deleted-<topic>-*            topic being deleted (removed at startup)
  groups/
    <group>.json                  committed consumer offsets (atomically replaced)
```

Topic creation builds the directory under `.creating-*` and renames it into
place; deletion renames it to `.deleted-*` first. Each rename (followed by a
directory fsync) is the commit point, so a crash never leaves a half-created
or half-deleted topic. `topic.json` and `<group>.json` are written to a
`.tmp` file, fsynced, renamed over the old file, and the directory fsynced.

## Segment record frame

A segment `.log` file is a sequence of frames. All integers are big-endian.

| Field | Type | Notes |
| --- | --- | --- |
| body length | uint32 | bytes after the 8-byte header; 28 B to 64 MiB |
| CRC-32C | uint32 | Castagnoli checksum of the body |
| magic | uint8 | record format version, currently `1` |
| attributes | uint8 | reserved, `0` (future compression codec bits) |
| offset | int64 | absolute partition offset |
| timestamp | int64 | broker append time, Unix milliseconds, never decreasing |
| key length | int32 | `-1` for a keyless record |
| key | bytes | |
| header count | uint16 | |
| headers | repeated | key length uint16, key, value length uint32, value |
| value length | uint32 | |
| value | bytes | |

A frame is valid only if its length is in range, the whole frame is present,
the CRC matches, the magic is known, the body parses exactly, and its offset
is the expected next offset. Every single-bit flip in a frame is detected
(see `TestFrameDetectsEverySingleBitFlip`).

## Sparse offset index

Each `.index` file holds 8-byte entries: offset relative to the segment base
(uint32) and byte position in the `.log` file (uint32). An entry is added for
a record whenever at least `--index-interval-bytes` (default 4 KiB) were
written since the previous entry. The same rule is used when appending and
when rebuilding, so a rebuilt index is byte-identical to the written one.
Reads binary-search the index and scan forward at most one interval.

Index files are derived data: they are never trusted over the log. A missing,
stale, or structurally invalid index is rebuilt at startup.

## Segments and rotation

The newest segment is the *active* segment; all others are *sealed*. The
active segment rolls when a batch would push it past `--segment-bytes`
(default 64 MiB, max 1 GiB) or when its first record is older than
`--segment-max-age` (default 24h, or the broker retention if shorter). On
roll the active segment's log and index are fsynced, then the new segment is
created and the directory fsynced. Sealed segments are therefore always fully
durable.

## Durability levels and acknowledgments

Publishers choose a durability level per request. The response states the
level that held; levels are never conflated.

| Level | Acknowledged when | Survives broker crash | Survives power loss / OS crash |
| --- | --- | --- | --- |
| `DURABILITY_WRITTEN` | appended to the OS page cache | yes | no, until the next fsync (at most `--flush-interval`, default 1s) |
| `DURABILITY_FSYNCED` (default) | appended and fsynced | yes | yes, unless the disk itself is lost |

There is no replicated (quorum) level until milestone R3. Concurrent FSYNCED
publishes share fsync calls (group commit). An fsync failure is treated as
fatal for that partition: it rejects further writes until restart, because the
kernel may have discarded the unwritten pages.

Delivery is at-least-once. A batch spanning partitions is not atomic, and a
retried publish whose first response was lost appends duplicates.

## Crash recovery

On startup, for every partition:

1. Leftover `.tmp` files and index files without a log are removed.
2. Each sealed segment's index is loaded and validated; records after the last
   index entry are verified, which also restores missing index entries. The
   segment must end exactly at the next segment's base offset. Any invalid
   frame, or a gap between segments, stops startup with a corruption error:
   sealed data was fsynced, so damage there is not a torn write and is never
   silently discarded.
3. The active segment is validated frame by frame. At the first invalid frame
   (torn write, garbage, zero-filled hole, out-of-sequence offset) the file is
   truncated, the truncation is logged with its position, size, and reason,
   and the `rift_storage_recovery_*` metrics are updated. The index is rebuilt
   and the segment fsynced.
4. Consumer-group offsets above the recovered end offset are lowered to it, so
   groups do not skip records later written at reused offsets.

Damage inside a sealed segment that startup does not scan is detected by the
CRC on read; the fetch reports `PARTITION_ERROR_CORRUPT_DATA` and the
`rift_storage_corrupt_reads_total` metric increases.

## Retention

Retention runs every `--retention-check-interval` (default 1m) and deletes
whole sealed segments, oldest first: by age when a segment's newest record is
older than the limit, and by size while the partition would still be at least
the limit without it. The active segment is never deleted. Limits come from
the topic (`retention_ms`, `retention_bytes`; `-1` = unlimited) or the broker
defaults (`--retention`, `--retention-bytes`; `0` = unlimited). Because only
sealed segments are deleted, time retention is coarse by up to one segment
age.

## Disk watermarks

Every 10s the broker samples the data directory's filesystem. Above
`--disk-high-watermark` (0.85) it logs a warning; above
`--disk-critical-watermark` (0.95) it rejects publishes with
`RESOURCE_EXHAUSTED` until usage drops, so the disk never fills mid-append.
Reads continue. `rift_disk_used_ratio` and `rift_disk_critical` expose the
state.

## Known limitations (R1)

- Compression is gzip on the gRPC transport (`--compress` / `WithCompression`);
  records are stored uncompressed.
- Consumers can read records as soon as they are written, before fsync. After
  power loss, a record a consumer saw under `DURABILITY_WRITTEN` may be gone
  and its offset reused by a new record.
- There is no time index, so consumers cannot seek by timestamp.
- Consumer groups have no membership or assignment until R4: two consumers in
  one group both read every partition they are given.
- No authentication or TLS. Disk-usage sampling requires a Unix platform.

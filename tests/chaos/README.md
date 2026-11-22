# Chaos and crash tests

`TestR1_CrashAtArbitraryWritePoint` is the R1 exit test. The test binary
re-executes itself as a writer that appends and fsyncs records, printing each
acknowledged offset. The parent SIGKILLs it at random times, then simulates
power loss by damaging only unacknowledged bytes of the active segment
(truncation, random garbage, or zero-filled holes), reopens the log, and
verifies that every acknowledged record survived and every recovered record is
contiguous and byte-identical. Iterations chain on the same log.

```sh
go test -race -v -run TestR1 ./tests/chaos
RIFT_CRASH_ITERATIONS=200 go test -run TestR1 ./tests/chaos   # longer soak
RIFT_REPORT_DIR=bin/reports go test -run TestR1 ./tests/chaos # JSON report
```

The seed is logged for each run. Network fault injection with Toxiproxy
arrives with multi-node milestones.

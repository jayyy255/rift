# Integration tests

These tests build `rift-broker` and `rift` and run them as real processes.

- `TestR1_BrokerCrashRecovery`: SIGKILL the broker under producer and
  consumer-group load, restart, and verify every acknowledged record, partition
  contiguity, per-key order, and committed offsets.
- `TestR1_GracefulShutdownKeepsWrittenRecords`: SIGTERM flushes `written`
  records that were not yet fsynced.
- `TestR1_CLIAgainstBrokerProcess`: the target CLI workflow from the spec.

```sh
go test -race -v ./tests/integration
```

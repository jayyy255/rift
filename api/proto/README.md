# Protocol definitions

Versioned Protobuf packages live under `rift/v1/`. Generated Go code is written to
`api/gen/` (package `riftv1`) and committed.

```sh
make proto        # buf generate (tools pinned in tools/go.mod)
make proto-check  # buf lint + fail if api/gen is stale
```

Currently defined: `ClusterService.GetStatus`. Publish acknowledgment policies,
stream offsets, and queue leases will be specified explicitly when R1 begins.
Lint rules (`STANDARD`) and breaking-change rules (`FILE`) are in `/buf.yaml`.

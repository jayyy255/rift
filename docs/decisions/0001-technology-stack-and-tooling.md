# 0001: Technology stack and tooling baseline

Status: accepted (2026-11-22)

## Context

The specification (section 5) fixes Go, gRPC/Protobuf, a Raft library for
metadata, Prometheus/Grafana/OpenTelemetry, Docker Compose, and a test stack of
race detector, fuzz/property tests, and Toxiproxy. The repository rule is to add
a library to `go.mod` only when its milestone needs it.

## Decision

**In `go.mod` now (used by code):**

| Concern | Module | Version |
| --- | --- | --- |
| RPC transport | `google.golang.org/grpc` | v1.85.0 |
| Wire schema | `google.golang.org/protobuf` | v1.36.12 |
| CLI | `github.com/spf13/cobra` | v1.10.2 |
| Metrics | `github.com/prometheus/client_golang` | v1.25.0 |
| Goroutine lifecycles | `golang.org/x/sync` (errgroup) | v0.25.0 |
| Structured logs | standard library `log/slog` | - |
| Property tests (added in R1) | `pgregory.net/rapid` | v1.3.0 |
| Disk usage (statfs) | `golang.org/x/sys` | (indirect via grpc) |

**Selected for later milestones (pre-fetched into the module cache, not yet in `go.mod`):**

| Milestone | Module | Version |
| --- | --- | --- |
| R2 metadata consensus | `github.com/hashicorp/raft`, `github.com/hashicorp/raft-boltdb/v2` | v1.8.0, v2.4.2 |
| Tracing | `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/sdk` | v1.47.0 |
| Chaos tests | `github.com/Shopify/toxiproxy/v2` (client) | v2.13.0 |
| R7 tiered storage | `github.com/minio/minio-go/v7` | v7.4.1 |

**Raft library: HashiCorp Raft** over etcd/raft. HashiCorp Raft provides
transport, log/stable stores, snapshotting and membership changes, which fits
"Raft for metadata only" with less plumbing. etcd/raft is a bare state machine
that would require writing the transport and storage loop. Revisit if
HashiCorp Raft's leadership-transfer or observer semantics block R2 exit tests.

**Code-generation and lint tools** are pinned in separate modules so their large
dependency graphs stay out of the broker's `go.mod`:

- `tools/go.mod`: buf v1.72.0, protoc-gen-go v1.36.12, protoc-gen-go-grpc v1.6.2.
  No system `protoc` is needed. buf v1.73+ requires Go 1.26.7; bump it together
  with the minimum Go version.
- `tools/lint/go.mod`: golangci-lint v2.15.0 (isolated because its dependencies
  conflict with grpc's genproto split).

Generated code lives in `api/gen` and is committed; `make proto-check` fails CI
on drift.

**Containers:** broker image built from `golang:1.26.11-alpine3.24` onto
`alpine:3.24`. The development stack (`deployments/compose/dev-deps.yaml`) runs
Prometheus v3.16.0, Grafana 13.2.6 and Toxiproxy 2.13.0. MinIO is deferred to R7:
its community container images are no longer published on Docker Hub, so the
image source must be chosen when tiered storage starts.

## Consequences

- Run tools with `go tool -modfile=tools/go.mod <tool>` (wrapped by `make`).
- Upgrading `google.golang.org/protobuf` should be matched by `protoc-gen-go`.
- Libraries in the "selected" table move into `go.mod` with the first code that uses them.

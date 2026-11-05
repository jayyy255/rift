# Contributing

Use small, focused branches and pull requests against `main`.

1. Read the architecture and current milestone in `docs/`.
2. Add a regression test when changing storage, ordering, delivery, or recovery behavior.
3. Run `make check` before opening a pull request.
4. Describe the behavior, validation, and any durability or compatibility impact.

Keep broker internals under `internal/`; reserve `pkg/client/` for the public SDK.
Keep generated Protobuf code reproducible and separate from hand-written code.
Do not commit credentials, local broker data, benchmark output, or compiled binaries.

The baseline has no third-party Go dependencies. Introduce libraries when their
implementation milestone needs them, and commit `go.sum` when dependencies produce it.

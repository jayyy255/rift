# Local cluster deployment

## Development dependencies (`dev-deps.yaml`)

`make dev-up` starts Prometheus (:9090), Grafana (:3000, anonymous admin, Prometheus
datasource provisioned) and Toxiproxy (:8474 API). Prometheus scrapes a broker
running on the host at `host.docker.internal:9100`; start it with `make run`, which
binds the metrics listener on all interfaces. `make dev-down` stops the stack.

## Three-broker cluster

A cluster service set will be added once metadata (R2) and replication (R3) exist.
Give each node a persistent volume and distinct identity, and document health
checks, ports, bootstrap membership, and clean shutdown with it.

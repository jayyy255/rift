# Configuration

The broker is configured with command-line flags (`rift-broker --help`).

```sh
rift-broker \
  --node-id rift-1 \
  --data-dir /var/lib/rift \
  --listen 0.0.0.0:7070 \
  --metrics-listen 0.0.0.0:9100 \
  --segment-bytes 67108864 \
  --retention 168h \
  --retention-bytes 0 \
  --flush-interval 1s \
  --disk-high-watermark 0.85 \
  --disk-critical-watermark 0.95
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--node-id` | `rift-1` | Broker identity, reported as partition leader |
| `--data-dir` | `./data` | Topic logs and consumer offsets |
| `--listen` / `--metrics-listen` | `127.0.0.1:7070` / `:9100` | gRPC and HTTP (`/metrics`, `/healthz`, `/readyz`) |
| `--segment-bytes` | 64 MiB | Roll size for segment files |
| `--segment-max-age` | 24h, or `--retention` if shorter | Roll age, which bounds time-retention granularity |
| `--index-interval-bytes` | 4 KiB | Sparse index spacing |
| `--retention` / `--retention-bytes` | 0 (unlimited) | Default per-partition retention; topics may override |
| `--retention-check-interval` | 1m | How often retention runs |
| `--flush-interval` | 1s | Longest time `written` records stay unsynced |
| `--max-record-bytes` / `--max-batch-bytes` | 1 MiB / 16 MiB | Request size limits |
| `--disk-high-watermark` / `--disk-critical-watermark` | 0.85 / 0.95 | Warn / reject publishes |
| `--log-level` | `info` | JSON logs on stderr |

Keep local overrides in `*.local.yaml` or scripts; never commit credentials.

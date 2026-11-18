# Tests

All end-to-end, fault-injection and benchmark tests are plain Go tests. They
start real `controller`/`datanode` subprocesses (built once per test binary)
through `tests/harness` on free localhost ports; no docker and no root needed.

```sh
go test ./... --short                  # fast: heavy cluster tests are skipped
go test -race ./tests/... -count=1     # e2e + fault injection + harness self-tests
go test ./tests/e2e -run TestE2E_PutGetDelete -v
```

| Package | Contents |
| --- | --- |
| `tests/harness` | Cluster harness (kill/restart, Raft edge proxies with cut/delay/drop/blackhole, readiness polling) and its self-tests |
| `tests/e2e` | Data path, topology and datanode restart behaviour |
| `tests/fault_injection` | Leader kill, partitions, delayed/dropped Raft messages, client timeouts |

Docker demos (`docker-compose.yml`, `run_local.sh`, `scripts/*.sh`) are kept for
manual use and are not part of the test suite.

## Benchmarks

```sh
go test ./tests/benchmark -run xxx -bench . -benchtime=1x       # one pass, prints p50/p95/p99
go test ./tests/benchmark -run xxx -bench ClientScaling -benchtime=5000x
go test -tags compare ./tests/benchmark -run xxx -bench Compare  # optional Redis/etcd (skips if unreachable)
```

Metrics reported per benchmark: `p50_ms p95_ms p99_ms max_ms ops/s err_rate`.
`go test --short` runs only the stub-backed smoke test (`TestLoadSmoke_Stub`) and
the percentile unit tests; `TestBench_ClusterSmoke` runs a tiny workload against
a real cluster in the non-short run.

# Limits and failure assumptions

Reference for Graphrun v1. Numbers come from `graphrun/src/limits.rs` and `docs/specs/v1/06-storage-and-consensus.md`.

## Hard limits

| Limit | Value |
|---|---|
| YAML size | 256 KiB |
| Payload size | 256 KiB |
| Schema size | 256 KiB |
| Normalized IR size | 256 KiB |
| Nodes per definition | 512 |
| Nesting depth | 8 |
| Data depth | 32 |
| Condition operators | 256 |
| Loop iterations | 1,000 |
| Parallel branches | 16 |
| Active leaves per run | 32 |
| Active leaves per worker | 128 |
| Foreach concurrency default / max | 8 / 32 |
| Activations per run | 10,000 |
| Active runs | 10,000 |
| Queued commands | 1,024 |
| Claim batch | 16 |
| Buffered events per run | 1,000 |
| Events per address | 100 |
| Name length | 64 |
| Unapplied log entries | 4,096 |
| Unapplied encoded bytes | 64 MiB |
| Snapshot entry threshold | 20,000 |
| Blocking pool | 16 |

A definition with more than 512 nodes fails validation. Client writes fail when unapplied entries reach 4,096 or unapplied encoded bytes reach 64 MiB.

## Failure model

Supported: crash-stop, partitions, machine restart, and power loss when the storage stack honors flushes.

Outside the model: Byzantine members, network filesystems, dishonest storage, and loss of all durable copies.

Restore creates a new cluster identity. Execution stays suspended until `graphrun resolve --ack`.

Compensation abandonment reports unresolved obligations. It does not report successful rollback.

## Performance

The spec target is 1,000 committed engine commands per second with p95 receipt latency under 100 ms on three same-region 4-vCPU / 8 GiB members with local durable SSDs and 1 KiB payloads.

If that hardware is not available, keep PERF-001 and PERF-002 `BLOCKED` and record the numbers from this host in `target/e2e-artifacts/PERF-001-measure.txt` and `PERF-002-measure.txt`. Do not treat a weaker host as the target.

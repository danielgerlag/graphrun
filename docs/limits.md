# Limits

Numbers come from `graphrun/src/limits.rs`.

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
| Foreach concurrency default / max | 8 / 32 |
| Activations per run | 10,000 |
| Unapplied log entries | 4,096 |
| Unapplied encoded bytes | 64 MiB |

A definition with more than 512 nodes fails validation. Client writes fail when unapplied entries reach 4,096 or unapplied encoded bytes reach 64 MiB.

Supported failures: crash-stop, partitions, machine restart, and power loss when the storage stack honors flushes. Not in the model: Byzantine members, network filesystems, dishonest storage, and loss of all durable copies.

Restore creates a new cluster identity. Execution stays suspended until recovery is acknowledged. Compensation abandonment reports unresolved obligations; it does not report successful rollback.

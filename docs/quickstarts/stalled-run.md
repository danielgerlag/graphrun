# Diagnose a stalled run

Inspect the run. Do not guess from logs.

```sh
./target/debug/graphrun inspect --run <run-id> --local-dir /tmp/graphrun-local
```

Read these fields in order:

1. `blocked_reason`. Why the run is not advancing, if it is still active.
2. `status`. `active` means the run is not terminal.
3. `pending_waits`. A row with `signal` and `key` means the run is waiting for `graphrun signal`. `deadline_ms` is the wait deadline.
4. `obligations`. `blocked` needs `resolve --blocked-input`. `irreversible` cannot undo. `compensating` is in-flight undo. `abandoned` was closed by an operator.
5. `recovery`. If `suspended` is true, the store was restored and needs `resolve --ack`.
6. `interventions`. Operator action still required.
7. `error`. Domain failure, not a transport timeout.

History is a separate command:

```sh
./target/debug/graphrun history --run <run-id> --local-dir /tmp/graphrun-local
./target/debug/graphrun replay --run <run-id> --local-dir /tmp/graphrun-local
```

`replay` is read-only. It does not start activities or write the store.

Cluster health:

```sh
./target/debug/graphrun cluster health --local-dir /tmp/graphrun-local
```

The JSON includes `clock_safe`, Raft `state`, `last_applied`, `last_log`, `apply_lag`, `unapplied_entries`, `voters`, `active_runs`, `ready_leaves`, and `inbox_depth`. A large `apply_lag` is apply lag. New commands fail when unapplied entries reach 4096 or unapplied encoded bytes reach 64 MiB. If `clock_safe` is false, writes fail closed.

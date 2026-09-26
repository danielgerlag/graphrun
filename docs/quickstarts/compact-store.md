# Compact an offline member

Stop the engine or member process that owns the data directory. Do not compact a live `member.redb`.

```sh
graphrun compact --local-dir ./graphrun-data
```

The command opens the existing database exclusively and runs at most 16 redb compaction passes. `status: "ok"` means no further pass is needed. If it reports `status: "incomplete"` and exits nonzero, run the same command again before starting the member.

Compaction does not change the active snapshot or prune workflow history. Use [Back up and restore](backup-restore.md) for disaster recovery, not a raw database copy.

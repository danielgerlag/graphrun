# Back up and restore

Backups are logical domain snapshots, not a copy of `member.redb`. Restore always creates a new cluster identity and leaves execution suspended until you acknowledge it.

This is ops on a data directory, not a new graph. Any run you already started (for example [`samples/02-passing-data/workflow.yaml`](../../samples/02-passing-data/workflow.yaml)) is in that snapshot.

Stop the engine first so the database is not open.

```sh
graphrun backup \
	--local-dir ./graphrun-data \
	--out ./graphrun-backup
```

The directory contains `manifest.json` and `domain.json`.

```sh
graphrun restore \
	--from ./graphrun-backup \
	--local-dir ./graphrun-restored \
	--confirm \
	--reason "disaster recovery drill"
```

Starts fail with `execution suspended until recovery is acknowledged` until you ack:

```sh
graphrun resolve \
	--run <any> \
	--ack \
	--reason "operator authorized restore" \
	--local-dir ./graphrun-restored
```

`--run` is ignored for `--ack`. Then open `Engine::local` (or `graphrun serve`) on the restored directory.

# Back up and restore a member

Backups are logical domain snapshots, not a copy of `member.redb`. Restore always creates a new cluster identity and leaves execution suspended until you acknowledge it.

## Back up

Stop the engine first so the database is not open.

```sh
./target/debug/graphrun backup \
	--local-dir /tmp/graphrun-local \
	--out /tmp/graphrun-backup
```

The directory contains `manifest.json` and `domain.json`.

## Restore

```sh
./target/debug/graphrun restore \
	--from /tmp/graphrun-backup \
	--local-dir /tmp/graphrun-restored \
	--confirm \
	--reason "disaster recovery drill"
```

`identity.json` on the destination uses a new `graphrun-restored-` cluster name. Starts fail with `execution suspended until recovery is acknowledged` until you ack:

```sh
./target/debug/graphrun resolve \
	--run <any> \
	--ack \
	--reason "operator authorized restore" \
	--local-dir /tmp/graphrun-restored
```

`--run` is ignored for `--ack`. Then `serve` the restored directory and continue existing runs.

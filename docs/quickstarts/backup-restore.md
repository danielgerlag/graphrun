# Back up and restore

Backups are logical domain snapshots, not a copy of `member.redb`. Restore always creates a new cluster identity and leaves execution suspended until you acknowledge it.

The member store uses `graphrun.member-store/v4` and `graphrun.state-record/v2`. Older member stores fail to open without losing their files. The logical backup envelope remains `graphrun.backup/v3`.

This is ops on a data directory, not a new graph. Any run you already started (for example [`samples/02-passing-data/workflow.yaml`](../../samples/02-passing-data/workflow.yaml)) is in that snapshot.

Stop the engine first so the database is not open.

After an unclean shutdown, reopen the engine before backing up. If redb needs repair, the engine validates a repaired copy of `member.redb` before it repairs the original file. An offline backup cannot repair a member store.

```sh
graphrun backup \
	--local-dir ./graphrun-data \
	--out ./graphrun-backup
```

The directory contains `manifest.json` and a checksummed, framed `application-<digest>.snap`. Keep both files. Restore verifies the digest, original artifact cluster IDs, retained catalogs and definitions, and every retained record version before it creates a new member directory. This includes publications that no run has used. Older backups without artifact origins are rejected without changing the destination.

To verify and inspect a backup without starting a member or calling an activity, use `graphrun::Engine::read_backup("./graphrun-backup")` from Rust. It returns the retained state only after the framing, artifact origins, and retained payload digests pass validation.

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

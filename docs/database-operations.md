# Database operations

## Retention cleanup

Retention is an explicit maintenance operation. It does not run inside the API or consumer, which
keeps request and stream-processing latency independent from cleanup work.

Preview eligible rows without deleting anything:

```bash
make retention-preview
```

Apply cleanup in bounded transactions:

```bash
make retention
```

The policy is configured in `.env`:

| Variable | Default | Records affected |
|---|---:|---|
| `READINGS_RETENTION_DAYS` | 90 | readings and their predictions |
| `DLQ_RETENTION_DAYS` | 30 | PostgreSQL dead-letter audit rows |
| `DRIFT_RETENTION_DAYS` | 90 | historical drift measurements |
| `OUTBOX_RETENTION_DAYS` | 7 | successfully published outbox rows only |
| `RETENTION_BATCH_SIZE` | 10000 | maximum rows deleted per transaction |

Pending outbox events are never eligible for retention, regardless of age. Schedule `make
retention` during a low-traffic period in the deployment platform after validating the preview.

## Backup

Create a PostgreSQL custom-format archive:

```bash
make backup
```

The command writes a timestamped file under `backups/`. It first writes a partial file, asks
`pg_restore` to parse the archive, and only then publishes the final filename. Backup archives are
ignored by Git and must be copied to encrypted off-machine storage by the deployment platform.

Verify an existing archive again:

```bash
python -m scripts.database verify backups/streamguard-YYYYMMDDTHHMMSSZ.dump
```

## Restore

Restore replaces the current schema. Stop all services that access PostgreSQL before running it,
leaving only the PostgreSQL container online:

```bash
docker compose stop producer consumer api dashboard monitoring
python -m scripts.database restore backups/streamguard-YYYYMMDDTHHMMSSZ.dump \
  --confirm-database streamguard
docker compose up -d
```

The archive is verified before restore, the confirmation must exactly equal the configured database
name, and `pg_restore` uses one transaction so an error cannot leave a partially restored schema.
Always rehearse restores against a separate non-production database and define recovery-time and
recovery-point objectives for the actual deployment.

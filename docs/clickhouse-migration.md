# ClickHouse Schema Migration Strategy

This document describes the strategy for managing ClickHouse schema migrations in SentinelMesh. It covers table versioning, step-by-step migration procedures, rollback, and post-migration validation.

## Overview

SentinelMesh uses ClickHouse for persistent storage of probe batches. The Aggregator connects to ClickHouse via HTTP (default port 8123) and runs `ensure_schema()` on startup to create tables if they don't exist. Data flows in through two paths:

- **Kafka Materialized View**: A `Kafka` engine table (`sentinelmesh_ingest_kafka`) feeds a `MergeTree` table (`probe_batches`) via a materialized view (`sentinelmesh_ingest_mv`).
- **Direct batch writes**: The `ClickHouseBatchWriter` inserts records via `INSERT INTO probe_batches FORMAT JSONEachRow`.

The current schema includes:

| Table | Engine | Purpose |
|---|---|---|
| `sentinelmesh_ingest_kafka` | Kafka | Consumes from Redpanda/Kafka topic |
| `probe_batches` | MergeTree | Primary storage for probe data |
| `sentinelmesh_ingest_mv` | Materialized View | Routes Kafka → probe_batches |

Schema changes must be coordinated carefully because the Kafka engine table, the materialized view, and the MergeTree table are tightly coupled.

## Prerequisites

- Access to the ClickHouse server via `clickhouse-client` or HTTP API
- The ClickHouse user configured in `aggregator.example.yaml` (default: `sentinelmesh`)
- Ability to stop and restart the Aggregator process
- A backup of the current data (or acceptance of data loss for the migration window)

## Table Versioning Strategy

### Schema Version Column

Every row in `probe_batches` carries a `schema_version` integer column. This value is set by the Agent when constructing a `ProbeBatch` and flows through Kafka into ClickHouse unchanged.

| schema_version | Description |
|---|---|
| 1 | Initial schema — base fields only |
| 2 | Added `asn` column, `transaction_order` in endpoints JSON |
| 3+ | Future additions |

When adding columns, always use a `DEFAULT` expression so that existing rows (with older `schema_version` values) remain queryable without backfill.

### Migration File Naming Convention

Store migration files in `deploy/clickhouse/` using a sequential numbering scheme:

```
deploy/clickhouse/
├── 001_init.sql
├── 002_add_asn_column.sql
├── 003_add_ttl_policy.sql
└── ...
```

Each file should be idempotent (use `IF NOT EXISTS`, `IF EXISTS` guards) so it can be safely re-run.

### Migration Metadata Table

Create a metadata table to track which migrations have been applied:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     UInt32,
    name        String,
    applied_at  DateTime DEFAULT now(),
    checksum    String
) ENGINE = MergeTree()
ORDER BY version;
```

Before applying a migration, check if it has already been applied:

```sql
SELECT version FROM schema_migrations WHERE version = 2;
```

After a successful migration, record it:

```sql
INSERT INTO schema_migrations (version, name, checksum)
VALUES (2, '002_add_asn_column', 'sha256:<hash>');
```

## Migration Procedure

### Step 1: Stop the Aggregator

Stop the Aggregator to prevent new writes during the migration. The Kafka consumer in ClickHouse will also pause because we will detach the materialized view.

```bash
systemctl stop sentinelmesh-aggregator
```

Verify it stopped:

```bash
systemctl status sentinelmesh-aggregator
```

### Step 2: Back Up the Affected Tables

Create a backup of the table you are about to modify. For `probe_batches`:

```sql
CREATE TABLE probe_batches_backup AS probe_batches
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sampled_at)
ORDER BY (sampled_at, sentinel_id);

INSERT INTO probe_batches_backup SELECT * FROM probe_batches;
```

Verify the backup row count matches:

```bash
clickhouse-client --query "SELECT count() FROM probe_batches"
clickhouse-client --query "SELECT count() FROM probe_batches_backup"
```

### Step 3: Detach the Materialized View

Detach the materialized view to prevent the Kafka engine table from writing to `probe_batches` during the migration:

```sql
DETACH TABLE sentinelmesh_ingest_mv;
```

This is critical — if the MV is active while you alter the target table, ClickHouse may reject the change or produce inconsistent data.

### Step 4: Apply the Schema Change

Run the migration SQL. Common operations:

#### Adding a Column

```sql
-- Example: add an ASN column with a default value
ALTER TABLE probe_batches
    ADD COLUMN IF NOT EXISTS asn UInt32 DEFAULT 0;
```

#### Modifying a Column Type

ClickHouse supports limited type changes. For safe type widening (e.g., `Int32` → `Int64`):

```sql
ALTER TABLE probe_batches
    MODIFY COLUMN schema_version Int64;
```

For incompatible type changes, create a new table and migrate data (see "Full Table Rebuild" below).

#### Adding a TTL Policy

```sql
ALTER TABLE probe_batches
    MODIFY TTL sampled_at + INTERVAL 90 DAY;
```

#### Creating a New Table

```sql
CREATE TABLE IF NOT EXISTS endpoint_samples (
    batch_id       UUID,
    sentinel_id    String,
    endpoint_id    String,
    sampled_at     DateTime64(3, 'UTC'),
    provider       String,
    payload        String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sampled_at)
ORDER BY (sampled_at, sentinel_id, endpoint_id);
```

### Step 5: Update the Kafka Engine Table and Materialized View (if needed)

If the migration added columns that should flow from Kafka, you need to recreate the Kafka engine table and the materialized view:

```sql
-- Drop the old Kafka engine table (it's stateless — no data loss)
DROP TABLE IF EXISTS sentinelmesh_ingest_kafka;

-- Recreate with the new column
CREATE TABLE sentinelmesh_ingest_kafka (
    batch_id           UUID,
    schema_version     Int32,
    sampled_at         DateTime64(3, 'UTC'),
    sentinel_id        String,
    sentinel_location  String,
    asn                UInt32,
    auth               String,
    endpoints          String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list  = 'sentinelmesh_ingest',
         kafka_group_name  = 'clickhouse_ingest',
         kafka_format      = 'JSONEachRow';

-- Recreate the materialized view
DROP TABLE IF EXISTS sentinelmesh_ingest_mv;

CREATE MATERIALIZED VIEW sentinelmesh_ingest_mv TO probe_batches AS
SELECT * FROM sentinelmesh_ingest_kafka;
```

If the migration did not change the Kafka-facing schema, simply reattach the existing MV:

```sql
ATTACH TABLE sentinelmesh_ingest_mv;
```

### Step 6: Record the Migration

```sql
INSERT INTO schema_migrations (version, name, checksum)
VALUES (2, '002_add_asn_column', 'sha256:abc123...');
```

### Step 7: Restart the Aggregator

```bash
systemctl start sentinelmesh-aggregator
```

Verify the Aggregator started and is writing data:

```bash
journalctl -u sentinelmesh-aggregator --since "2 minutes ago" | grep -i "schema\|clickhouse\|started"
```

## Full Table Rebuild

For incompatible schema changes (e.g., changing the `ORDER BY` key, changing partition expression, or incompatible type changes), use a full table rebuild:

```sql
-- 1. Create the new table with the desired schema
CREATE TABLE probe_batches_v2 (
    batch_id           UUID,
    schema_version     Int64,          -- widened type
    sampled_at         DateTime64(3, 'UTC'),
    sentinel_id        String,
    sentinel_location  String,
    asn                UInt32 DEFAULT 0,
    auth               String,
    endpoints          String,
    received_at        DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sampled_at)
ORDER BY (sampled_at, sentinel_id);

-- 2. Copy data from the old table
INSERT INTO probe_batches_v2 SELECT * FROM probe_batches;

-- 3. Verify row counts match
SELECT count() FROM probe_batches;
SELECT count() FROM probe_batches_v2;

-- 4. Swap tables atomically
RENAME TABLE probe_batches TO probe_batches_old,
             probe_batches_v2 TO probe_batches;

-- 5. Recreate the materialized view pointing to the new table
DROP TABLE IF EXISTS sentinelmesh_ingest_mv;
CREATE MATERIALIZED VIEW sentinelmesh_ingest_mv TO probe_batches AS
SELECT * FROM sentinelmesh_ingest_kafka;

-- 6. After validation, drop the old table
-- DROP TABLE probe_batches_old;
```

## Rollback Procedure

### If the Migration Failed Mid-Way

If the `ALTER TABLE` or table creation failed, ClickHouse typically leaves the table in its original state. Verify:

```bash
clickhouse-client --query "DESCRIBE TABLE probe_batches"
```

If the table is corrupted or in an unexpected state, restore from backup:

```sql
-- Drop the broken table
DROP TABLE IF EXISTS probe_batches;

-- Restore from backup
RENAME TABLE probe_batches_backup TO probe_batches;
```

Reattach the materialized view:

```sql
ATTACH TABLE sentinelmesh_ingest_mv;
```

### If the Migration Succeeded but Caused Issues

If the new schema causes application errors (e.g., the Aggregator fails to hydrate):

1. Stop the Aggregator:

```bash
systemctl stop sentinelmesh-aggregator
```

2. Revert the schema change. For column additions:

```sql
ALTER TABLE probe_batches
    DROP COLUMN IF EXISTS asn;
```

3. If you used a full table rebuild, swap back:

```sql
RENAME TABLE probe_batches TO probe_batches_broken,
             probe_batches_old TO probe_batches;
```

4. Recreate the Kafka engine table and MV if they were changed (use the previous definitions).

5. Remove the migration record:

```sql
ALTER TABLE schema_migrations DELETE WHERE version = 2;
```

6. Restart the Aggregator:

```bash
systemctl start sentinelmesh-aggregator
```

### If Data Was Lost

If the Kafka consumer group offset advanced during the migration window and some messages were consumed but not materialized:

1. Reset the ClickHouse Kafka consumer group offset to replay missed messages:

```sql
-- Drop and recreate the Kafka engine table to reset offsets
DROP TABLE IF EXISTS sentinelmesh_ingest_kafka;
-- Recreate with the same definition (see Step 5 above)
```

2. Alternatively, if the Kafka retention period has not expired, the messages are still available. Recreating the Kafka engine table with a new `kafka_group_name` will replay from the earliest offset:

```sql
CREATE TABLE sentinelmesh_ingest_kafka (
    -- ... same columns ...
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:29092',
         kafka_topic_list  = 'sentinelmesh_ingest',
         kafka_group_name  = 'clickhouse_ingest_replay',  -- new group name
         kafka_format      = 'JSONEachRow';
```

## Post-Migration Validation

After every migration, run these checks before considering the migration complete.

### 1. Verify Table Structure

```bash
clickhouse-client --query "DESCRIBE TABLE probe_batches"
```

Confirm the new columns, types, and defaults match expectations.

### 2. Verify Row Counts

```bash
clickhouse-client --query "SELECT count() FROM probe_batches"
```

Compare with the pre-migration count. For additive changes (new columns), the count should be identical.

### 3. Verify Data Integrity

Check that existing data is still readable and new defaults are applied:

```sql
-- Verify old rows have the default value for new columns
SELECT schema_version, count()
FROM probe_batches
GROUP BY schema_version
ORDER BY schema_version;

-- Spot-check a few rows
SELECT batch_id, sentinel_id, sampled_at, asn
FROM probe_batches
ORDER BY sampled_at DESC
LIMIT 10;
```

### 4. Verify Materialized View Is Active

```sql
SELECT name, engine
FROM system.tables
WHERE database = 'sentinelmesh' AND engine = 'MaterializedView';
```

Confirm `sentinelmesh_ingest_mv` is listed.

### 5. Verify New Data Is Flowing

Wait 1–2 minutes after restarting the Aggregator, then check for fresh rows:

```sql
SELECT count()
FROM probe_batches
WHERE received_at >= now() - INTERVAL 2 MINUTE;
```

If the count is 0 and the Aggregator is running, check the Aggregator logs and the Kafka consumer lag.

### 6. Verify Aggregator Hydration

```bash
# The Aggregator hydrates from ClickHouse on startup
curl -s http://localhost:9480/v1/snapshot | jq '.samples | length'
```

The sample count should be non-zero if there is recent data in ClickHouse.

### 7. Check for ClickHouse Errors

```sql
SELECT event_time, message
FROM system.text_log
WHERE level = 'Error'
  AND event_time >= now() - INTERVAL 10 MINUTE
ORDER BY event_time DESC
LIMIT 20;
```

### 8. Verify Migration Metadata

```sql
SELECT * FROM schema_migrations ORDER BY version;
```

Confirm the latest migration is recorded.

## ClickHouse-Specific Best Practices

### MergeTree Engine Considerations

- Always define `PARTITION BY` for time-series data. SentinelMesh uses `toYYYYMM(sampled_at)` for monthly partitions.
- The `ORDER BY` clause defines the primary index. Changing it requires a full table rebuild — plan carefully.
- Use `OPTIMIZE TABLE probe_batches FINAL` after large data migrations to force merge of parts, but avoid running this in production during peak load.

### TTL for Data Retention

Configure TTL to automatically expire old data:

```sql
ALTER TABLE probe_batches
    MODIFY TTL sampled_at + INTERVAL 90 DAY;
```

ClickHouse will drop expired partitions in the background. Verify TTL is active:

```sql
SELECT name, engine_full
FROM system.tables
WHERE name = 'probe_batches';
```

### Materialized Views

- Materialized views in ClickHouse are insert triggers — they only process new data, not existing rows.
- If you change a MV's `SELECT` query, you must `DROP` and `CREATE` it again. Existing data in the target table is not affected.
- When adding a column to the target table that should be populated from Kafka, you must also update the Kafka engine table and the MV.

### Column Defaults and Nullable Types

- Prefer `DEFAULT` expressions over `Nullable` types. `Nullable` adds storage overhead and complicates queries.
- Use `DEFAULT 0`, `DEFAULT ''`, or `DEFAULT now()` for new columns so that old rows are queryable without backfill.

### Testing Migrations

Before applying a migration to production:

1. Test on a staging ClickHouse instance with a copy of production data.
2. Verify the Aggregator's `ensure_schema()` still works after the migration (it uses `IF NOT EXISTS` guards).
3. Run the post-migration validation queries on staging.
4. Measure the migration duration on staging to estimate the production downtime window.

## Checklist

- [ ] Migration SQL file created in `deploy/clickhouse/` with sequential numbering
- [ ] Migration tested on staging environment
- [ ] Aggregator stopped
- [ ] Backup of affected tables created and verified
- [ ] Materialized view detached
- [ ] Schema change applied
- [ ] Kafka engine table and MV updated (if needed)
- [ ] Migration recorded in `schema_migrations` table
- [ ] Aggregator restarted
- [ ] Post-migration validation passed (structure, row counts, data integrity)
- [ ] New data flowing through MV confirmed
- [ ] Aggregator hydration verified
- [ ] No ClickHouse errors in logs
- [ ] Backup table dropped after validation period (recommended: 48 hours)

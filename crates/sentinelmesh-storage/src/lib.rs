use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use sentinelmesh_core::{EndpointSample, ProbeEnvelope, ReplayLogConfig, StorageConfig};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions, types::Json};
use tracing::{info, warn};

pub struct StorageEngine {
    pool: PgPool,
    replay_log: Option<ReplayLog>,
}

impl StorageEngine {
    pub async fn connect(config: &StorageConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.database.max_connections)
            .connect(&config.database.postgres_url)
            .await
            .context("failed to connect to PostgreSQL")?;

        let replay_log = config.replay_log.as_ref().map(ReplayLog::new).transpose()?;

        Ok(Self { pool, replay_log })
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        for statement in [
            r"
            CREATE TABLE IF NOT EXISTS probe_batches (
                batch_id UUID PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                sampled_at TIMESTAMPTZ NOT NULL,
                sentinel_id TEXT NOT NULL,
                sentinel_location TEXT NOT NULL,
                auth JSONB NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            ",
            r"
            CREATE TABLE IF NOT EXISTS endpoint_samples (
                batch_id UUID NOT NULL REFERENCES probe_batches(batch_id) ON DELETE CASCADE,
                sentinel_id TEXT NOT NULL,
                endpoint_id TEXT NOT NULL,
                sampled_at TIMESTAMPTZ NOT NULL,
                provider TEXT NOT NULL,
                payload JSONB NOT NULL,
                PRIMARY KEY (batch_id, sentinel_id, endpoint_id)
            )
            ",
            r"
            CREATE INDEX IF NOT EXISTS endpoint_samples_sampled_at_idx
            ON endpoint_samples (sampled_at DESC)
            ",
            r"
            CREATE INDEX IF NOT EXISTS endpoint_samples_provider_idx
            ON endpoint_samples (provider)
            ",
        ] {
            sqlx::query(statement)
                .execute(&self.pool)
                .await
                .context("failed to ensure storage schema")?;
        }

        Ok(())
    }

    pub async fn persist_envelope(&self, envelope: &ProbeEnvelope) -> Result<bool> {
        self.persist_envelope_inner(envelope, true).await
    }

    async fn persist_envelope_inner(
        &self,
        envelope: &ProbeEnvelope,
        append_replay: bool,
    ) -> Result<bool> {
        if append_replay {
            if let Some(replay_log) = &self.replay_log {
                replay_log.append(envelope)?;
            }
        }

        let mut transaction = self.pool.begin().await?;
        let auth = envelope
            .auth
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .context("failed to serialize batch auth to json")?;

        let result = sqlx::query(
            r"
            INSERT INTO probe_batches (
                batch_id, schema_version, sampled_at, sentinel_id, sentinel_location, auth
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (batch_id) DO NOTHING
            ",
        )
        .bind(envelope.batch.batch_id)
        .bind(i32::from(envelope.batch.schema_version))
        .bind(envelope.batch.sampled_at)
        .bind(envelope.batch.sentinel_id.as_str())
        .bind(envelope.batch.sentinel_location.as_str())
        .bind(auth)
        .execute(&mut *transaction)
        .await
        .context("failed to insert probe batch")?;

        let inserted = result.rows_affected() == 1;
        if inserted {
            for sample in envelope.batch.clone().into_samples() {
                sqlx::query(
                    r"
                    INSERT INTO endpoint_samples (
                        batch_id, sentinel_id, endpoint_id, sampled_at, provider, payload
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (batch_id, sentinel_id, endpoint_id) DO NOTHING
                    ",
                )
                .bind(sample.batch_id)
                .bind(sample.sentinel_id.as_str())
                .bind(sample.observation.endpoint.id.as_str())
                .bind(sample.sampled_at)
                .bind(sample.observation.endpoint.provider.as_str())
                .bind(Json(&sample))
                .execute(&mut *transaction)
                .await
                .with_context(|| {
                    format!(
                        "failed to insert endpoint sample {} for batch {}",
                        sample.observation.endpoint.id, sample.batch_id
                    )
                })?;
            }
        }

        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn hydrate_recent_samples(&self, window: Duration) -> Result<Vec<EndpointSample>> {
        let cutoff = Utc::now() - chrono::Duration::from_std(window).unwrap_or_default();
        let rows = sqlx::query(
            r"
            SELECT payload
            FROM endpoint_samples
            WHERE sampled_at >= $1
            ORDER BY sampled_at DESC
            ",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate recent samples from storage")?;

        rows.into_iter()
            .map(|row| {
                let payload: Value = row.try_get("payload")?;
                serde_json::from_value(payload).context("failed to decode endpoint sample payload")
            })
            .collect()
    }

    pub async fn replay_from_log(&self) -> Result<usize> {
        let Some(replay_log) = &self.replay_log else {
            return Ok(0);
        };

        if !replay_log.replay_on_startup {
            return Ok(0);
        }

        let mut restored = 0_usize;
        for envelope in replay_log.read_all()? {
            if self.persist_envelope_inner(&envelope, false).await? {
                restored += 1;
            }
        }

        if restored > 0 {
            info!(
                restored_batches = restored,
                "replayed batches from local log"
            );
        }

        Ok(restored)
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

pub struct ReplayLog {
    path: PathBuf,
    replay_on_startup: bool,
}

impl ReplayLog {
    pub fn new(config: &ReplayLogConfig) -> Result<Self> {
        let path = PathBuf::from(&config.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create replay log directory {}", parent.display())
            })?;
        }

        Ok(Self {
            path,
            replay_on_startup: config.replay_on_startup,
        })
    }

    pub fn append(&self, envelope: &ProbeEnvelope) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("failed to open replay log {}", self.path.display()))?;
        serde_json::to_writer(&mut file, envelope)
            .context("failed to serialize probe envelope to replay log")?;
        writeln!(file).context("failed to append newline to replay log")?;
        file.flush().context("failed to flush replay log")?;
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<ProbeEnvelope>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)
            .with_context(|| format!("failed to open replay log {}", self.path.display()))?;
        let reader = BufReader::new(file);
        let mut envelopes = Vec::new();

        for (line_number, line) in reader.lines().enumerate() {
            let line = line
                .with_context(|| format!("failed to read replay log line {}", line_number + 1))?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<ProbeEnvelope>(&line) {
                Ok(envelope) => envelopes.push(envelope),
                Err(error) => warn!(
                    line_number = line_number + 1,
                    error = %error,
                    path = %self.path.display(),
                    "skipping malformed replay log line"
                ),
            }
        }

        Ok(envelopes)
    }
}

pub fn sample_replay_log_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join("sentinelmesh-replay.ndjson")
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sentinelmesh_core::{
        BatchAuth, EndpointObservation, ProbeBatch, ProbeEnvelope, ProbeValue, RpcEndpointConfig,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{ReplayLog, sample_replay_log_path};

    #[test]
    fn replay_log_round_trips() {
        let tempdir = tempdir().expect("tempdir should exist");
        let replay_path = sample_replay_log_path(tempdir.path());
        let replay_log = ReplayLog {
            path: replay_path.clone(),
            replay_on_startup: true,
        };

        replay_log
            .append(&sample_envelope())
            .expect("append should succeed");
        let envelopes = replay_log.read_all().expect("read should succeed");
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].batch.sentinel_id, "sentinel-a");
        assert!(replay_path.exists());
    }

    fn sample_envelope() -> ProbeEnvelope {
        ProbeEnvelope {
            batch: ProbeBatch {
                schema_version: 2,
                batch_id: Uuid::new_v4(),
                sampled_at: Utc::now(),
                sentinel_id: "sentinel-a".to_owned(),
                sentinel_location: "lab".to_owned(),
                endpoints: vec![EndpointObservation {
                    endpoint: RpcEndpointConfig {
                        id: "endpoint-a".to_owned(),
                        label: "endpoint-a".to_owned(),
                        provider: "provider-a".to_owned(),
                        region: "region-a".to_owned(),
                        rpc_url: "http://localhost:8899".to_owned(),
                        tags: std::collections::BTreeMap::default(),
                    },
                    overall_latency_ms: 4,
                    health: ProbeValue::ok("ok".to_owned(), 1),
                    slot: ProbeValue::ok(10, 1),
                    block_height: ProbeValue::ok(11, 1),
                    latest_blockhash: ProbeValue::err("not needed", 1),
                    version: ProbeValue::ok("1.0".to_owned(), 1),
                    identity: ProbeValue::err("not needed", 1),
                    vote_accounts: ProbeValue::err("not needed", 1),
                    cluster_nodes: ProbeValue::err("not needed", 1),
                    leader_schedule: ProbeValue::err("not needed", 1),
                    accounts: Vec::new(),
                    signatures: Vec::new(),
                    probe_errors: Vec::new(),
                }],
            },
            auth: Some(BatchAuth {
                signer_id: "sentinel-a".to_owned(),
                key_id: "key-1".to_owned(),
                signed_at: Utc::now(),
                batch_hash: "hash".to_owned(),
                signature_b64: "sig".to_owned(),
            }),
        }
    }
}

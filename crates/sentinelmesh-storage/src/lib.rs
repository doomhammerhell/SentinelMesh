#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use reqwest::{Client as HttpClient, Url};
use rskafka::{
    client::{Client, ClientBuilder, partition::PartitionClient},
    record::Record,
};
use sentinelmesh_core::{EndpointSample, ProbeEnvelope, StorageConfig};
use serde_json::Value;
use tracing::warn;

/// Deterministically select a partition for a given key using blake3 hash.
///
/// Returns a partition index in `[0, num_partitions)`.
#[must_use]
pub fn partition_for_key(key: &str, num_partitions: u32) -> i32 {
    if num_partitions <= 1 {
        return 0;
    }
    let hash = blake3::hash(key.as_bytes());
    let bytes = hash.as_bytes();
    let value = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    i32::try_from(value % num_partitions).unwrap_or(0)
}

pub struct StorageEngine {
    #[allow(dead_code)]
    kafka_client: Client,
    partition_clients: Vec<PartitionClient>,
    num_partitions: u32,
    clickhouse_client: HttpClient,
    clickhouse_url: Url,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
}

impl StorageEngine {
    pub async fn connect(config: &StorageConfig) -> Result<Self> {
        let kafka_client = ClientBuilder::new(config.kafka.brokers.clone())
            .build()
            .await
            .context("failed to connect to Kafka brokers")?;

        let controller_client = kafka_client
            .controller_client()
            .context("failed to get Kafka controller client")?;

        let num_partitions = config.kafka.partitions.max(1);

        // Ensure topic exists with the configured number of partitions
        if let Err(error) = controller_client
            .create_topic(
                &config.kafka.topic,
                i32::try_from(num_partitions).unwrap_or(1),
                1,
                5_000,
            )
            .await
        {
            warn!(error = %error, "Topic creation returned an error (it might already exist)");
        }

        // Create one PartitionClient per partition
        let mut partition_clients = Vec::with_capacity(num_partitions as usize);
        for partition_idx in 0..i32::try_from(num_partitions).unwrap_or(1) {
            let pc = kafka_client
                .partition_client(
                    &config.kafka.topic,
                    partition_idx,
                    rskafka::client::partition::UnknownTopicHandling::Retry,
                )
                .await
                .with_context(|| {
                    format!("failed to create partition client for partition {partition_idx}")
                })?;
            partition_clients.push(pc);
        }

        let clickhouse_client = HttpClient::new();
        let clickhouse_url =
            Url::parse(&config.clickhouse.url).context("invalid clickhouse url")?;

        Ok(Self {
            kafka_client,
            partition_clients,
            num_partitions,
            clickhouse_client,
            clickhouse_url,
            clickhouse_user: config.clickhouse.user.clone(),
            clickhouse_password: config.clickhouse.password.clone(),
        })
    }

    async fn execute_clickhouse(&self, query: &str) -> Result<()> {
        let mut req = self
            .clickhouse_client
            .post(self.clickhouse_url.clone())
            .body(query.to_owned());
        if let (Some(user), Some(password)) = (&self.clickhouse_user, &self.clickhouse_password) {
            req = req.basic_auth(user, Some(password));
        }

        let res = req
            .send()
            .await
            .context("failed to execute clickhouse query")?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse error {status}: {body}"));
        }
        Ok(())
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        let queries = [
            r"
            CREATE TABLE IF NOT EXISTS sentinelmesh_ingest_kafka (
                batch_id UUID,
                schema_version Int32,
                sampled_at DateTime64(3, 'UTC'),
                sentinel_id String,
                sentinel_location String,
                auth String,
                endpoints String
            ) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'redpanda:29092,host.docker.internal:9092',
                     kafka_topic_list = 'sentinelmesh_ingest',
                     kafka_group_name = 'clickhouse_ingest',
                     kafka_format = 'JSONEachRow';
            ",
            r"
            CREATE TABLE IF NOT EXISTS probe_batches (
                batch_id UUID,
                schema_version Int32,
                sampled_at DateTime64(3, 'UTC'),
                sentinel_id String,
                sentinel_location String,
                auth String,
                endpoints String,
                received_at DateTime64(3, 'UTC') DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(sampled_at)
            ORDER BY (sampled_at, sentinel_id);
            ",
            r"
            CREATE MATERIALIZED VIEW IF NOT EXISTS sentinelmesh_ingest_mv TO probe_batches AS
            SELECT * FROM sentinelmesh_ingest_kafka;
            ",
        ];

        for query in queries {
            self.execute_clickhouse(query).await?;
        }

        Ok(())
    }

    pub async fn persist_envelope(&self, envelope: &ProbeEnvelope) -> Result<bool> {
        #[derive(serde::Serialize)]
        struct KafkaPayload<'a> {
            batch_id: uuid::Uuid,
            schema_version: i32,
            sampled_at: DateTime<Utc>,
            sentinel_id: &'a str,
            sentinel_location: &'a str,
            asn: Option<u32>,
            auth: Option<String>,
            endpoints: String,
        }

        let auth_str = envelope
            .auth
            .as_ref()
            .map(|a| serde_json::to_string(a).unwrap_or_default());
        let endpoints_str = serde_json::to_string(&envelope.batch.endpoints).unwrap_or_default();

        let payload = KafkaPayload {
            batch_id: envelope.batch.batch_id,
            schema_version: i32::from(envelope.batch.schema_version),
            sampled_at: envelope.batch.sampled_at,
            sentinel_id: &envelope.batch.sentinel_id,
            sentinel_location: &envelope.batch.sentinel_location,
            asn: envelope.batch.asn,
            auth: auth_str,
            endpoints: endpoints_str,
        };

        let json_bytes =
            serde_json::to_vec(&payload).context("failed to serialize envelope for kafka")?;

        let record = Record {
            key: Some(envelope.batch.sentinel_id.as_bytes().to_vec()),
            value: Some(json_bytes),
            headers: std::collections::BTreeMap::default(),
            timestamp: chrono::Utc::now(),
        };

        // Select partition based on sentinel_id
        let partition_idx =
            partition_for_key(&envelope.batch.sentinel_id, self.num_partitions) as usize;
        let client = &self.partition_clients[partition_idx];

        client
            .produce(
                vec![record],
                rskafka::client::partition::Compression::NoCompression,
            )
            .await
            .context("failed to produce record to Redpanda/Kafka")?;

        Ok(true)
    }

    pub async fn hydrate_recent_samples(&self, window: Duration) -> Result<Vec<EndpointSample>> {
        let cutoff = Utc::now() - chrono::Duration::from_std(window).unwrap_or_default();
        let query = format!(
            "SELECT sentinel_id, asn, endpoints, batch_id, sampled_at FROM probe_batches WHERE sampled_at >= '{}' ORDER BY sampled_at DESC FORMAT JSONEachRow",
            cutoff.format("%Y-%m-%d %H:%M:%S")
        );

        let mut req = self
            .clickhouse_client
            .post(self.clickhouse_url.clone())
            .body(query);
        if let (Some(user), Some(password)) = (&self.clickhouse_user, &self.clickhouse_password) {
            req = req.basic_auth(user, Some(password));
        }

        let res = req
            .send()
            .await
            .context("failed to execute clickhouse query")?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse error {status}: {body}"));
        }

        let text = res.text().await?;
        let mut samples = Vec::new();

        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Value = serde_json::from_str(line)
                .context("failed to decode JSONEachRow from ClickHouse")?;

            let sentinel_id = row
                .get("sentinel_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let asn = row
                .get("asn")
                .and_then(serde_json::Value::as_u64)
                .map(|v| u32::try_from(v).unwrap_or(u32::MAX));
            let batch_id = row
                .get("batch_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let sampled_at_str = row
                .get("sampled_at")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let endpoints_raw = row
                .get("endpoints")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let parsed_batch_id = uuid::Uuid::parse_str(batch_id).unwrap_or_default();
            let parsed_sampled_at = chrono::DateTime::parse_from_str(
                &format!("{sampled_at_str} +0000"),
                "%Y-%m-%d %H:%M:%S%.f %z",
            )
            .map_or_else(|_| Utc::now(), |d| d.with_timezone(&Utc));

            if let Ok(observations) =
                serde_json::from_str::<Vec<sentinelmesh_core::EndpointObservation>>(endpoints_raw)
            {
                for obs in observations {
                    samples.push(EndpointSample {
                        batch_id: parsed_batch_id,
                        sentinel_id: sentinel_id.clone(),
                        sentinel_location: String::new(),
                        asn,
                        sampled_at: parsed_sampled_at,
                        hlc: sentinelmesh_core::Hlc::new(parsed_sampled_at.timestamp_millis(), 0),
                        observation: obs,
                    });
                }
            }
        }

        Ok(samples)
    }
    pub async fn list_agents(&self) -> Result<Vec<String>> {
        let query = "SELECT DISTINCT sentinel_id FROM probe_batches FORMAT JSONEachRow";

        let mut req = self
            .clickhouse_client
            .post(self.clickhouse_url.clone())
            .body(query);
        if let (Some(user), Some(password)) = (&self.clickhouse_user, &self.clickhouse_password) {
            req = req.basic_auth(user, Some(password));
        }

        let res = req
            .send()
            .await
            .context("failed to execute list_agents query")?;
        if !res.status().is_success() {
            return Ok(Vec::new());
        }

        let text = res.text().await?;
        let mut agents = Vec::new();
        for line in text.lines() {
            if let Ok(row) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(id) = row.get("sentinel_id").and_then(|v| v.as_str()) {
                    agents.push(id.to_owned());
                }
            }
        }
        Ok(agents)
    }

    #[allow(clippy::unused_async)]
    pub async fn replay_from_log(&self) -> Result<usize> {
        // Redpanda has native durable retention. Local replay log logic is deprecated.
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// ClickHouse Batch Writer (R8)
// ---------------------------------------------------------------------------

/// Metrics counters for the batch writer, exposed as Prometheus gauges/counters.
#[derive(Debug, Default, Clone)]
pub struct BatchWriterMetrics {
    /// Current number of items in the buffer.
    pub buffer_size: usize,
    /// Latency of the last successful flush in milliseconds.
    pub last_flush_latency_ms: u64,
    /// Total number of flush failures.
    pub flush_failures_total: u64,
}

/// Result of a flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Flush was successful — all records were written.
    Success,
    /// Flush failed — records are retained for retry.
    Failure,
    /// Nothing to flush — buffer was empty.
    NothingToFlush,
}

/// A batch writer that accumulates serialized records and flushes them when
/// either `batch_size` is reached or `batch_timeout` has elapsed.
///
/// The actual write operation is delegated to a caller-supplied closure /
/// function so that the struct can be tested independently of `ClickHouse`.
pub struct ClickHouseBatchWriter<F>
where
    F: FnMut(&[String]) -> Result<()>,
{
    buffer: Vec<String>,
    batch_size: usize,
    batch_timeout: Duration,
    last_flush: Instant,
    flush_fn: F,
    pub metrics: BatchWriterMetrics,
}

impl<F> ClickHouseBatchWriter<F>
where
    F: FnMut(&[String]) -> Result<()>,
{
    /// Create a new batch writer.
    ///
    /// * `batch_size`    – flush when the buffer reaches this many records.
    /// * `batch_timeout` – flush when this duration has elapsed since the last flush.
    /// * `flush_fn`      – the function that performs the actual write (e.g. `ClickHouse` INSERT).
    pub fn new(batch_size: usize, batch_timeout: Duration, flush_fn: F) -> Self {
        Self {
            buffer: Vec::new(),
            batch_size: batch_size.max(1),
            batch_timeout,
            last_flush: Instant::now(),
            flush_fn,
            metrics: BatchWriterMetrics::default(),
        }
    }

    /// Push a JSON-serialized record into the buffer.
    ///
    /// Returns `Some(FlushOutcome)` if a flush was triggered by reaching
    /// `batch_size`, or `None` if the record was simply buffered.
    pub fn push(&mut self, record: String) -> Option<FlushOutcome> {
        self.buffer.push(record);
        self.metrics.buffer_size = self.buffer.len();

        if self.buffer.len() >= self.batch_size {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Attempt to flush if `batch_timeout` has elapsed since the last flush.
    ///
    /// Call this periodically (e.g. from a timer tick) to ensure records are
    /// not stuck in the buffer indefinitely.
    pub fn try_flush_on_timeout(&mut self) -> Option<FlushOutcome> {
        if self.buffer.is_empty() {
            return None;
        }
        if self.last_flush.elapsed() >= self.batch_timeout {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Force a flush of the current buffer contents.
    ///
    /// On failure the records are **retained** in the buffer for retry.
    pub fn flush(&mut self) -> FlushOutcome {
        if self.buffer.is_empty() {
            return FlushOutcome::NothingToFlush;
        }

        let start = Instant::now();
        if let Ok(()) = (self.flush_fn)(&self.buffer) {
            let elapsed = start.elapsed();
            self.metrics.last_flush_latency_ms = elapsed.as_millis() as u64;
            self.buffer.clear();
            self.metrics.buffer_size = 0;
            self.last_flush = Instant::now();
            FlushOutcome::Success
        } else {
            self.metrics.flush_failures_total += 1;
            // Records are retained in the buffer for retry.
            FlushOutcome::Failure
        }
    }

    /// Current number of buffered records.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Read-only view of the current buffer contents.
    pub fn buffer_contents(&self) -> &[String] {
        &self.buffer
    }

    /// Elapsed time since the last successful flush (or creation).
    pub fn elapsed_since_last_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Feature: sentinelmesh-comprehensive-upgrade, Property 9: Determinismo de particionamento Kafka
    // **Validates: Requirements 7.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_kafka_partition_determinism(
            sentinel_id in "[a-zA-Z0-9_-]{1,64}",
            num_partitions in 1u32..=256,
        ) {
            // Same sentinel_id must always map to the same partition
            let p1 = partition_for_key(&sentinel_id, num_partitions);
            let p2 = partition_for_key(&sentinel_id, num_partitions);
            prop_assert_eq!(p1, p2, "partition_for_key must be deterministic");

            // Partition must be in valid range [0, num_partitions)
            prop_assert!(p1 >= 0, "partition must be non-negative");
            prop_assert!((p1 as u32) < num_partitions, "partition must be < num_partitions");
        }

        #[test]
        fn prop_kafka_partition_determinism_pairs(
            id_a in "[a-zA-Z0-9_-]{1,64}",
            id_b in "[a-zA-Z0-9_-]{1,64}",
            num_partitions in 1u32..=256,
        ) {
            // Both IDs must produce deterministic results
            let pa1 = partition_for_key(&id_a, num_partitions);
            let pa2 = partition_for_key(&id_a, num_partitions);
            prop_assert_eq!(pa1, pa2, "id_a must be deterministic");

            let pb1 = partition_for_key(&id_b, num_partitions);
            let pb2 = partition_for_key(&id_b, num_partitions);
            prop_assert_eq!(pb1, pb2, "id_b must be deterministic");

            // If IDs are the same, partitions must be the same
            if id_a == id_b {
                prop_assert_eq!(pa1, pb1, "same ID must map to same partition");
            }

            // All partitions must be in valid range
            prop_assert!((pa1 as u32) < num_partitions);
            prop_assert!((pb1 as u32) < num_partitions);
        }
    }

    #[test]
    fn partition_for_key_single_partition() {
        // With 1 partition, everything maps to 0
        assert_eq!(partition_for_key("any-key", 1), 0);
        assert_eq!(partition_for_key("another-key", 1), 0);
    }

    #[test]
    fn partition_for_key_zero_partitions() {
        // Edge case: 0 partitions should return 0 (handled by early return)
        assert_eq!(partition_for_key("key", 0), 0);
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 10: Triggers de flush do batch ClickHouse
    // **Validates: Requisito 8.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_batch_flush_on_batch_size(
            batch_size in 1usize..=50,
            num_records in 1usize..=200,
        ) {
            use std::cell::RefCell;

            let flush_count = RefCell::new(0usize);
            let flush_fn = |_records: &[String]| -> Result<()> {
                *flush_count.borrow_mut() += 1;
                Ok(())
            };

            let mut writer = ClickHouseBatchWriter::new(
                batch_size,
                Duration::from_secs(3600), // very long timeout — only batch_size should trigger
                flush_fn,
            );

            for i in 0..num_records {
                let record = format!("{{\"id\":{i}}}");
                writer.push(record);
            }

            let expected_flushes = num_records / batch_size;
            let expected_remaining = num_records % batch_size;

            prop_assert_eq!(
                *flush_count.borrow(),
                expected_flushes,
                "flush should occur exactly num_records / batch_size times"
            );
            prop_assert_eq!(
                writer.buffer_len(),
                expected_remaining,
                "remaining buffer should be num_records % batch_size"
            );
        }

        #[test]
        fn prop_batch_flush_on_timeout(
            batch_size in 10usize..=100,
            num_records in 1usize..9,
        ) {
            // Insert fewer records than batch_size, then trigger timeout flush
            let flush_fn = |_records: &[String]| -> Result<()> { Ok(()) };

            let mut writer = ClickHouseBatchWriter::new(
                batch_size,
                Duration::from_millis(0), // immediate timeout
                flush_fn,
            );

            for i in 0..num_records {
                writer.push(format!("{{\"id\":{i}}}"));
            }

            // Buffer should still have records (batch_size not reached)
            prop_assert_eq!(writer.buffer_len(), num_records);

            // Now trigger timeout-based flush
            let outcome = writer.try_flush_on_timeout();
            prop_assert_eq!(outcome, Some(FlushOutcome::Success));
            prop_assert_eq!(writer.buffer_len(), 0, "buffer should be empty after timeout flush");
        }
    }

    #[test]
    fn batch_flush_timeout_not_elapsed() {
        let flush_fn = |_records: &[String]| -> Result<()> { Ok(()) };
        let mut writer = ClickHouseBatchWriter::new(
            100,
            Duration::from_secs(3600), // very long timeout
            flush_fn,
        );

        writer.push("record".to_string());
        let outcome = writer.try_flush_on_timeout();
        assert_eq!(
            outcome, None,
            "should not flush when timeout hasn't elapsed"
        );
        assert_eq!(writer.buffer_len(), 1);
    }

    #[test]
    fn batch_flush_empty_buffer() {
        let flush_fn = |_records: &[String]| -> Result<()> { Ok(()) };
        let mut writer = ClickHouseBatchWriter::new(10, Duration::from_secs(1), flush_fn);

        assert_eq!(writer.flush(), FlushOutcome::NothingToFlush);
        assert_eq!(writer.try_flush_on_timeout(), None);
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 11: Retry de batch em caso de falha
    // **Validates: Requisito 8.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_batch_retry_on_failure(
            num_records in 1usize..=50,
        ) {
            use std::cell::RefCell;

            let should_fail = RefCell::new(true);
            let flush_fn = |_records: &[String]| -> Result<()> {
                if *should_fail.borrow() {
                    Err(anyhow!("simulated ClickHouse failure"))
                } else {
                    Ok(())
                }
            };

            let mut writer = ClickHouseBatchWriter::new(
                num_records + 1, // batch_size > num_records so push won't auto-flush
                Duration::from_secs(3600),
                flush_fn,
            );

            // Insert records
            for i in 0..num_records {
                writer.push(format!("{{\"id\":{i}}}"));
            }
            prop_assert_eq!(writer.buffer_len(), num_records);

            // First flush attempt fails — records must be retained
            let outcome = writer.flush();
            prop_assert_eq!(outcome, FlushOutcome::Failure);
            prop_assert_eq!(
                writer.buffer_len(),
                num_records,
                "records must be retained in buffer after failure"
            );
            prop_assert!(
                writer.metrics.flush_failures_total >= 1,
                "failure counter must be incremented"
            );

            // Verify the same records are still available
            let contents = writer.buffer_contents();
            prop_assert_eq!(contents.len(), num_records);
            for (i, record) in contents.iter().enumerate() {
                prop_assert_eq!(record, &format!("{{\"id\":{i}}}"));
            }

            // Now allow flush to succeed — retry should work
            *should_fail.borrow_mut() = false;
            let outcome = writer.flush();
            prop_assert_eq!(outcome, FlushOutcome::Success);
            prop_assert_eq!(writer.buffer_len(), 0, "buffer should be empty after successful retry");
        }

        #[test]
        fn prop_batch_multiple_failures_retain_all(
            num_records in 1usize..=30,
            num_failures in 1usize..=5,
        ) {
            use std::cell::RefCell;

            let fail_count = RefCell::new(0usize);
            let max_failures = num_failures;
            let flush_fn = |_records: &[String]| -> Result<()> {
                let mut count = fail_count.borrow_mut();
                if *count < max_failures {
                    *count += 1;
                    Err(anyhow!("simulated failure"))
                } else {
                    Ok(())
                }
            };

            let mut writer = ClickHouseBatchWriter::new(
                num_records + 1,
                Duration::from_secs(3600),
                flush_fn,
            );

            for i in 0..num_records {
                writer.push(format!("{{\"id\":{i}}}"));
            }

            // Fail num_failures times — buffer must retain all records each time
            for attempt in 0..num_failures {
                let outcome = writer.flush();
                prop_assert_eq!(outcome, FlushOutcome::Failure, "attempt {} should fail", attempt);
                prop_assert_eq!(
                    writer.buffer_len(),
                    num_records,
                    "buffer must retain all records after failure attempt {}",
                    attempt
                );
            }

            prop_assert_eq!(
                writer.metrics.flush_failures_total,
                num_failures as u64,
                "failure counter must match number of failures"
            );

            // Final attempt succeeds
            let outcome = writer.flush();
            prop_assert_eq!(outcome, FlushOutcome::Success);
            prop_assert_eq!(writer.buffer_len(), 0);
        }
    }
}

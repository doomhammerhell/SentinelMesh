use std::time::Duration;

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

pub struct StorageEngine {
    #[allow(dead_code)]
    kafka_client: Client,
    partition_client: PartitionClient,
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

        // Ensure topic exists
        if let Err(error) = controller_client
            .create_topic(&config.kafka.topic, 1, 1, 5_000)
            .await
        {
            warn!(error = %error, "Topic creation returned an error (it might already exist)");
        }

        let partition_client = kafka_client
            .partition_client(
                &config.kafka.topic,
                0,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            )
            .await
            .context("failed to create partition client for Kafka topic")?;

        let clickhouse_client = HttpClient::new();
        let clickhouse_url =
            Url::parse(&config.clickhouse.url).context("invalid clickhouse url")?;

        Ok(Self {
            kafka_client,
            partition_client,
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

        self.partition_client
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
                        sentinel_location: String::new(), // Context omitted in DB flattening, assume irrelevant for hydratation dashboard
                        asn,
                        sampled_at: parsed_sampled_at,
                        observation: obs,
                    });
                }
            }
        }

        Ok(samples)
    }

    #[allow(clippy::unused_async)]
    pub async fn replay_from_log(&self) -> Result<usize> {
        // Redpanda has native durable retention. Local replay log logic is deprecated.
        Ok(0)
    }
}

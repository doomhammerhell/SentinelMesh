use reqwest::Client;
use sentinelmesh_core::{AlertsConfig, Anomaly};
use std::{collections::HashMap, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct AlertSink {
    sender: mpsc::Sender<Vec<Anomaly>>,
}

impl AlertSink {
    pub fn new(config: AlertsConfig) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Vec<Anomaly>>(128);

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        tokio::spawn(async move {
            let mut last_sent: HashMap<String, Instant> = HashMap::new();
            let ratelimit_window = Duration::from_secs(900); // 15 minutos cooldown por código

            while let Some(anomalies) = receiver.recv().await {
                for anomaly in anomalies {
                    if anomaly.severity < config.min_severity {
                        continue;
                    }

                    let now = Instant::now();
                    if let Some(last) = last_sent.get(&anomaly.code) {
                        if now.duration_since(*last) < ratelimit_window {
                            continue; // cooldown ativo
                        }
                    }

                    // Prepara Payload genérico no design do Slack/PagerDuty
                    let payload = serde_json::json!({
                        "text": format!("🚨 *SentinelMesh Alert*: [{:?}] {}", anomaly.severity, anomaly.summary),
                        "severity": anomaly.severity,
                        "code": anomaly.code,
                    });

                    for webhook in &config.webhooks {
                        let mut request = client.post(&webhook.url).json(&payload);
                        for (k, v) in &webhook.headers {
                            request = request.header(k, v);
                        }

                        let response: Result<reqwest::Response, reqwest::Error> =
                            request.send().await;
                        match response {
                            Ok(res) => {
                                if res.status().is_success() {
                                    info!(code = %anomaly.code, dest_url = %webhook.url, "alert dispatched successfully");
                                } else {
                                    warn!(code = %anomaly.code, status = %res.status(), dest_url = %webhook.url, "webhook returned non-success status");
                                }
                            }
                            Err(e) => {
                                error!(code = %anomaly.code, error = %e, dest_url = %webhook.url, "failed to dispatch alert to webhook");
                            }
                        }
                    }

                    last_sent.insert(anomaly.code.clone(), now);
                }
            }
        });

        Self { sender }
    }

    pub fn dispatch(&self, anomalies: Vec<Anomaly>) {
        if anomalies.is_empty() {
            return;
        }
        let _ = self.sender.try_send(anomalies);
    }
}

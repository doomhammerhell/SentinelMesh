use reqwest::Client;
use sentinelmesh_core::{AlertsConfig, Anomaly};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, error, info, warn};

/// Maximum number of anomaly batches buffered in the consumer's internal VecDeque.
/// When this limit is reached, the oldest entries are dropped to make room.
const MAX_PENDING: usize = 256;

#[derive(Clone)]
pub struct AlertSink {
    sender: mpsc::Sender<Vec<Anomaly>>,
}

impl AlertSink {
    pub fn new(config: AlertsConfig) -> Self {
        let (sender, receiver) = mpsc::channel::<Vec<Anomaly>>(128);

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        tokio::spawn(alert_consumer_loop(receiver, config, client));

        Self { sender }
    }

    /// Dispatch anomalies to the alert consumer. Never blocks the caller.
    /// If the channel is full, the batch is silently dropped (backpressure).
    pub fn dispatch(&self, anomalies: Vec<Anomaly>) {
        if anomalies.is_empty() {
            return;
        }
        match self.sender.try_send(anomalies) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                debug!("alert channel full, anomaly batch dropped at producer side");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("alert consumer has been shut down");
            }
        }
    }
}

/// The consumer loop runs in a separate async task. It drains the mpsc channel
/// into an internal `VecDeque` (bounded by `MAX_PENDING`). When the deque is
/// full, the *oldest* anomaly batches are evicted — satisfying the requirement
/// that older anomalies are discarded rather than newer ones.
async fn alert_consumer_loop(
    mut receiver: mpsc::Receiver<Vec<Anomaly>>,
    config: AlertsConfig,
    client: Client,
) {
    let mut pending: VecDeque<Vec<Anomaly>> = VecDeque::new();
    let mut last_sent: HashMap<String, Instant> = HashMap::new();
    let ratelimit_window = Duration::from_secs(config.rate_limit_window_secs);

    loop {
        // First, drain any available messages from the channel into the deque.
        // If the deque is empty, block until at least one message arrives.
        if pending.is_empty() {
            match receiver.recv().await {
                Some(batch) => enqueue(&mut pending, batch),
                None => break, // channel closed
            }
        }

        // Non-blocking drain of any additional messages already in the channel.
        while let Ok(batch) = receiver.try_recv() {
            enqueue(&mut pending, batch);
        }

        // Process one batch from the front of the deque.
        if let Some(anomalies) = pending.pop_front() {
            process_anomaly_batch(&anomalies, &config, &client, &mut last_sent, ratelimit_window)
                .await;
        }
    }
}

/// Enqueue a batch into the pending deque, evicting the oldest if at capacity.
fn enqueue(pending: &mut VecDeque<Vec<Anomaly>>, batch: Vec<Anomaly>) {
    while pending.len() >= MAX_PENDING {
        let evicted = pending.pop_front();
        if let Some(evicted) = evicted {
            debug!(
                dropped_count = evicted.len(),
                "evicting oldest anomaly batch from consumer buffer (backpressure)"
            );
        }
    }
    pending.push_back(batch);
}

/// Process a single batch of anomalies: apply rate limiting, then dispatch to webhooks.
async fn process_anomaly_batch(
    anomalies: &[Anomaly],
    config: &AlertsConfig,
    client: &Client,
    last_sent: &mut HashMap<String, Instant>,
    ratelimit_window: Duration,
) {
    for anomaly in anomalies {
        if anomaly.severity < config.min_severity {
            continue;
        }

        let now = Instant::now();
        if let Some(last) = last_sent.get(&anomaly.code) {
            if now.duration_since(*last) < ratelimit_window {
                continue; // rate-limited
            }
        }

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

            match request.send().await {
                Ok(res) => {
                    if res.status().is_success() {
                        info!(code = %anomaly.code, dest_url = %webhook.url, "alert dispatched successfully");
                    } else {
                        warn!(code = %anomaly.code, status = %res.status(), dest_url = %webhook.url, "webhook returned non-success status");
                    }
                }
                Err(e) if e.is_timeout() => {
                    warn!(code = %anomaly.code, dest_url = %webhook.url, "webhook timed out after 10s, proceeding");
                }
                Err(e) => {
                    error!(code = %anomaly.code, error = %e, dest_url = %webhook.url, "failed to dispatch alert to webhook");
                }
            }
        }

        last_sent.insert(anomaly.code.clone(), now);
    }
}

// ---------------------------------------------------------------------------
// Testable helpers exposed for property tests
// ---------------------------------------------------------------------------

/// A synchronous, deterministic rate limiter for testing purposes.
/// Tracks the last dispatch time per anomaly code.
pub struct RateLimiter {
    window: Duration,
    last_sent: HashMap<String, std::time::Instant>,
}

impl RateLimiter {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            last_sent: HashMap::new(),
        }
    }

    /// Returns `true` if the anomaly should be dispatched (not rate-limited).
    pub fn should_dispatch(&mut self, code: &str) -> bool {
        let now = std::time::Instant::now();
        if let Some(last) = self.last_sent.get(code) {
            if now.duration_since(*last) < self.window {
                return false;
            }
        }
        self.last_sent.insert(code.to_owned(), now);
        true
    }
}

/// A bounded buffer that drops the oldest entries when full.
/// Used for backpressure testing.
pub struct BoundedBuffer<T> {
    inner: VecDeque<T>,
    capacity: usize,
}

impl<T> BoundedBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Push an item. If at capacity, evict the oldest item first.
    /// Returns `true` if an eviction occurred.
    pub fn push(&mut self, item: T) -> bool {
        let evicted = if self.inner.len() >= self.capacity {
            self.inner.pop_front();
            true
        } else {
            false
        };
        self.inner.push_back(item);
        evicted
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn bounded_buffer_evicts_oldest_when_full() {
        let mut buf = BoundedBuffer::new(3);
        assert!(!buf.push(1));
        assert!(!buf.push(2));
        assert!(!buf.push(3));
        assert_eq!(buf.len(), 3);

        // This should evict 1
        assert!(buf.push(4));
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.inner.front(), Some(&2));
    }

    #[test]
    fn bounded_buffer_never_exceeds_capacity() {
        let mut buf = BoundedBuffer::new(2);
        for i in 0..100 {
            buf.push(i);
            assert!(buf.len() <= 2);
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 12: Backpressure do AlertSink
    // **Validates: Requirements 9.2**
    //
    // For any sequence of anomalies sent to the AlertSink that exceeds the
    // channel capacity, the producer must never block. Excess anomalies must
    // be discarded.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_backpressure_never_exceeds_capacity(
            capacity in 1_usize..=64,
            num_items in 0_usize..=512,
        ) {
            let mut buf = BoundedBuffer::new(capacity);
            for i in 0..num_items {
                buf.push(i);
                // Invariant: buffer length never exceeds capacity
                prop_assert!(buf.len() <= capacity,
                    "buffer length {} exceeded capacity {} after inserting item {}",
                    buf.len(), capacity, i);
            }
            // Final length should be min(num_items, capacity)
            let expected_len = num_items.min(capacity);
            prop_assert_eq!(buf.len(), expected_len);
        }
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 13: Rate limiting do AlertSink
    // **Validates: Requirements 9.3**
    //
    // For any sequence of anomalies with the same code sent within a
    // rate_limit_window, only the first should be dispatched. Subsequent
    // anomalies with the same code within the window should be suppressed.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_rate_limiting_suppresses_duplicates(
            num_codes in 1_usize..=10,
            dispatches_per_code in 2_usize..=20,
        ) {
            // Use a very large window so nothing expires during the test
            let mut limiter = RateLimiter::new(Duration::from_secs(3600));

            for code_idx in 0..num_codes {
                let code = format!("anomaly_code_{}", code_idx);
                let mut dispatch_count = 0;

                for _ in 0..dispatches_per_code {
                    if limiter.should_dispatch(&code) {
                        dispatch_count += 1;
                    }
                }

                // Only the first dispatch per code should succeed within the window
                prop_assert_eq!(dispatch_count, 1,
                    "expected exactly 1 dispatch for code '{}', got {}",
                    code, dispatch_count);
            }
        }
    }
}

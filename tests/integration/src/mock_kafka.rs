//! Mock Kafka for integration tests.
//!
//! Provides an in-memory implementation of produce/consume semantics,
//! allowing tests to verify message flow without a real Kafka broker.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// A single record stored in the mock Kafka topic.
#[derive(Clone, Debug)]
pub struct MockRecord {
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

/// In-memory partition holding an ordered sequence of records.
#[derive(Clone, Debug, Default)]
struct Partition {
    records: Vec<MockRecord>,
    next_offset: u64,
}

/// In-memory Kafka topic with multiple partitions.
#[derive(Clone)]
pub struct MockKafkaTopic {
    inner: Arc<RwLock<TopicInner>>,
}

struct TopicInner {
    name: String,
    num_partitions: u32,
    partitions: HashMap<u32, Partition>,
}

impl MockKafkaTopic {
    /// Create a new mock topic with the given number of partitions.
    pub fn new(name: &str, num_partitions: u32) -> Self {
        let mut partitions = HashMap::new();
        for i in 0..num_partitions {
            partitions.insert(i, Partition::default());
        }
        Self {
            inner: Arc::new(RwLock::new(TopicInner {
                name: name.to_string(),
                num_partitions,
                partitions,
            })),
        }
    }

    /// Produce a record to a specific partition.
    pub fn produce(&self, partition: u32, key: Option<String>, value: Vec<u8>) -> u64 {
        let mut inner = self.inner.write();
        let part = inner
            .partitions
            .get_mut(&partition)
            .expect("partition out of range");
        let offset = part.next_offset;
        part.records.push(MockRecord {
            key,
            value,
            partition,
            offset,
        });
        part.next_offset += 1;
        offset
    }

    /// Produce a record, selecting partition by key hash (mirrors real Kafka behavior).
    pub fn produce_with_key(&self, key: &str, value: Vec<u8>) -> (u32, u64) {
        let partition = {
            let inner = self.inner.read();
            key_to_partition(key, inner.num_partitions)
        };
        let offset = self.produce(partition, Some(key.to_string()), value);
        (partition, offset)
    }

    /// Consume all records from a specific partition starting at the given offset.
    pub fn consume(&self, partition: u32, from_offset: u64) -> Vec<MockRecord> {
        let inner = self.inner.read();
        inner
            .partitions
            .get(&partition)
            .map(|p| {
                p.records
                    .iter()
                    .filter(|r| r.offset >= from_offset)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Consume all records across all partitions.
    pub fn consume_all(&self) -> Vec<MockRecord> {
        let inner = self.inner.read();
        let mut all: Vec<MockRecord> = inner
            .partitions
            .values()
            .flat_map(|p| p.records.iter().cloned())
            .collect();
        all.sort_by_key(|r| (r.partition, r.offset));
        all
    }

    /// Get the total number of records across all partitions.
    pub fn total_records(&self) -> usize {
        let inner = self.inner.read();
        inner.partitions.values().map(|p| p.records.len()).sum()
    }

    /// Get the topic name.
    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }

    /// Get the number of partitions.
    pub fn num_partitions(&self) -> u32 {
        self.inner.read().num_partitions
    }
}

/// Deterministic partition selection using blake3 hash of the key,
/// mirroring `partition_for_key` in sentinelmesh-storage.
fn key_to_partition(key: &str, num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let hash = blake3::hash(key.as_bytes());
    let bytes = hash.as_bytes();
    let value = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    value % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_and_consume_single_partition() {
        let topic = MockKafkaTopic::new("test-topic", 1);
        topic.produce(0, Some("key1".into()), b"value1".to_vec());
        topic.produce(0, Some("key2".into()), b"value2".to_vec());

        let records = topic.consume(0, 0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, b"value1");
        assert_eq!(records[1].value, b"value2");
    }

    #[test]
    fn produce_with_key_distributes_deterministically() {
        let topic = MockKafkaTopic::new("test-topic", 4);
        let (p1, _) = topic.produce_with_key("sentinel-a", b"v1".to_vec());
        let (p2, _) = topic.produce_with_key("sentinel-a", b"v2".to_vec());
        // Same key always goes to same partition
        assert_eq!(p1, p2);
    }

    #[test]
    fn consume_from_offset() {
        let topic = MockKafkaTopic::new("test-topic", 1);
        topic.produce(0, None, b"a".to_vec());
        topic.produce(0, None, b"b".to_vec());
        topic.produce(0, None, b"c".to_vec());

        let records = topic.consume(0, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, b"b");
    }

    #[test]
    fn consume_all_across_partitions() {
        let topic = MockKafkaTopic::new("test-topic", 2);
        topic.produce(0, None, b"p0-a".to_vec());
        topic.produce(1, None, b"p1-a".to_vec());
        topic.produce(0, None, b"p0-b".to_vec());

        let all = topic.consume_all();
        assert_eq!(all.len(), 3);
    }
}

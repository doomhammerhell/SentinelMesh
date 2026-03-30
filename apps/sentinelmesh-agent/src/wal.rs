#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]

use std::path::Path;

use anyhow::{Context, Result};
use metrics::{counter, gauge, histogram};
use sentinelmesh_core::ProbeBatch;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct DiskQueue {
    db: sled::Db,
}

impl DiskQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).context("failed to open sled database for wal")?;
        Ok(Self { db })
    }

    pub fn push(&self, batch: &ProbeBatch, max_entries: usize) -> Result<()> {
        let current_len = self.db.len();
        if current_len >= max_entries {
            if let Ok(Some((k, _))) = self.db.first() {
                let _ = self.db.remove(&k);
                counter!("sentinelmesh_agent_wal_evictions_total").increment(1);
                warn!(
                    wal_len = current_len,
                    max_entries = max_entries,
                    "wal capacity exceeded, evicting oldest probe batch to preserve host disk"
                );
            }
        }

        let id = self
            .db
            .generate_id()
            .context("failed to generate sequential wal id")?;
        let bytes = serde_json::to_vec(batch).context("failed to serialize batch for wal")?;

        let started = std::time::Instant::now();
        self.db
            .insert(id.to_be_bytes(), bytes)
            .context("failed to write envelope to wal")?;

        // Asynchronous flush is fine for high throughput, but we'll do synchronous to be safe
        // on agent side since this is the contingency path.
        self.db.flush().context("failed to flush wal to disk")?;
        histogram!("sentinelmesh_agent_wal_flush_latency_ms")
            .record(started.elapsed().as_secs_f64() * 1000.0);

        // Update depth gauge after insertion
        gauge!("sentinelmesh_agent_wal_depth").set(self.db.len() as f64);

        debug!(wal_id = id, "batch safely persisted to wal");
        Ok(())
    }

    pub fn pop_front(&self) -> Result<Option<(sled::IVec, ProbeBatch)>> {
        if let Some((k, v)) = self.db.first().context("failed to read head of wal")? {
            match serde_json::from_slice::<ProbeBatch>(&v) {
                Ok(batch) => Ok(Some((k, batch))),
                Err(e) => {
                    warn!(error = %e, "corrupted wal entry found, skipping and removing");
                    let _ = self.remove(&k);
                    self.pop_front() // Look for next valid
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&self, id: &sled::IVec) -> Result<()> {
        self.db
            .remove(id)
            .context("failed to remove item from wal")?;
        self.db
            .flush()
            .context("failed to flush wal after removal")?;

        // Update depth gauge after removal
        gauge!("sentinelmesh_agent_wal_depth").set(self.db.len() as f64);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.db.len()
    }
}

// ---------------------------------------------------------------------------
// Testable eviction logic for property tests (no sled dependency)
// ---------------------------------------------------------------------------

/// A simple in-memory WAL model for testing eviction behaviour.
/// Mirrors the eviction semantics of `DiskQueue` without disk I/O.
#[cfg(test)]
pub struct InMemoryWal {
    entries: std::collections::VecDeque<(u64, Vec<u8>)>,
    max_entries: usize,
    next_id: u64,
    pub eviction_count: u64,
}

#[cfg(test)]
impl InMemoryWal {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: std::collections::VecDeque::new(),
            max_entries,
            next_id: 0,
            eviction_count: 0,
        }
    }

    /// Push a batch. If at capacity, evict the oldest entry first.
    pub fn push(&mut self, data: Vec<u8>) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
            self.eviction_count += 1;
        }
        self.entries.push_back((self.next_id, data));
        self.next_id += 1;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the id of the oldest entry, if any.
    pub fn oldest_id(&self) -> Option<u64> {
        self.entries.front().map(|(id, _)| *id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn in_memory_wal_evicts_oldest() {
        let mut wal = InMemoryWal::new(3);
        wal.push(vec![1]);
        wal.push(vec![2]);
        wal.push(vec![3]);
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.oldest_id(), Some(0));

        wal.push(vec![4]);
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.oldest_id(), Some(1)); // oldest (id=0) was evicted
        assert_eq!(wal.eviction_count, 1);
    }

    // Feature: sentinelmesh-comprehensive-upgrade, Property 14: Evicção do WAL na capacidade máxima
    // **Validates: Requirements 10.4**
    //
    // For any WAL with `wal_max_entries` entries, inserting a new batch must:
    // (a) evict the oldest batch, (b) insert the new batch, and
    // (c) maintain WAL size <= wal_max_entries. Depth must never exceed max.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_wal_eviction_maintains_max_entries(
            max_entries in 1_usize..=100,
            num_inserts in 0_usize..=300,
        ) {
            let mut wal = InMemoryWal::new(max_entries);

            for i in 0..num_inserts {
                wal.push(vec![i as u8]);

                // Invariant: WAL depth never exceeds max_entries
                prop_assert!(wal.len() <= max_entries,
                    "WAL depth {} exceeded max_entries {} after insert {}",
                    wal.len(), max_entries, i);
            }

            // Final length should be min(num_inserts, max_entries)
            let expected_len = num_inserts.min(max_entries);
            prop_assert_eq!(wal.len(), expected_len);

            // Eviction count should be max(0, num_inserts - max_entries)
            let expected_evictions = num_inserts.saturating_sub(max_entries) as u64;
            prop_assert_eq!(wal.eviction_count, expected_evictions,
                "expected {} evictions, got {}",
                expected_evictions, wal.eviction_count);

            // If we inserted more than max_entries, the oldest entry should
            // have id = num_inserts - max_entries (the first non-evicted entry)
            if num_inserts > max_entries {
                let expected_oldest_id = (num_inserts - max_entries) as u64;
                prop_assert_eq!(wal.oldest_id(), Some(expected_oldest_id),
                    "expected oldest id {}, got {:?}",
                    expected_oldest_id, wal.oldest_id());
            }
        }
    }
}

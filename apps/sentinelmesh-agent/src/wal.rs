use std::path::Path;

use anyhow::{Context, Result};
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

        self.db
            .insert(id.to_be_bytes(), bytes)
            .context("failed to write envelope to wal")?;

        // Asynchronous flush is fine for high throughput, but we'll do synchronous to be safe
        // on agent side since this is the contingency path.
        self.db.flush().context("failed to flush wal to disk")?;

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
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.db.len()
    }
}

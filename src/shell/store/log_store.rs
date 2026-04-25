use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use tokio::sync::RwLock;

use crate::TypeConfig;

type NID = u64;

fn io_err(e: impl std::fmt::Display) -> StorageError<NID> {
    StorageError::IO {
        source: StorageIOError::new(
            openraft::ErrorSubject::Logs,
            openraft::ErrorVerb::Write,
            anyerror::AnyError::error(e.to_string()),
        ),
    }
}

/// Sled-backed Raft log store. Three sled trees:
/// - `log`   : index (u64 BE bytes) → Entry<TypeConfig> (JSON)
/// - `meta`  : "vote" → Vote (JSON), "last_purged" → LogId (JSON)
#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<sled::Db>,
    /// Cache of last_purged to avoid constant deserialization.
    last_purged: Arc<RwLock<Option<LogId<NID>>>>,
}

impl LogStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path.as_ref().join("raft-log"))?;
        // Recover last_purged from meta tree
        let meta = db.open_tree("meta")?;
        let last_purged = meta
            .get("last_purged")
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(Self {
            db: Arc::new(db),
            last_purged: Arc::new(RwLock::new(last_purged)),
        })
    }

    fn log_tree(&self) -> sled::Tree {
        self.db.open_tree("log").expect("open log tree")
    }

    fn meta_tree(&self) -> sled::Tree {
        self.db.open_tree("meta").expect("open meta tree")
    }

    fn index_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NID>> {
        let log = self.log_tree();
        let start = match range.start_bound() {
            std::ops::Bound::Included(&v) => Self::index_key(v).to_vec(),
            std::ops::Bound::Excluded(&v) => Self::index_key(v + 1).to_vec(),
            std::ops::Bound::Unbounded => vec![],
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&v) => std::ops::Bound::Included(Self::index_key(v).to_vec()),
            std::ops::Bound::Excluded(&v) => std::ops::Bound::Excluded(Self::index_key(v).to_vec()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };

        let mut entries = Vec::new();
        let iter = log.range((std::ops::Bound::Included(start), end));
        for item in iter {
            let (_, val) = item.map_err(|e| io_err(&e))?;
            let entry: Entry<TypeConfig> = serde_json::from_slice(&val).map_err(|e| io_err(&e))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NID>> {
        let log = self.log_tree();
        let last_log_id = log
            .last()
            .map_err(|e| io_err(&e))?
            .map(|(_, v)| serde_json::from_slice::<Entry<TypeConfig>>(&v))
            .transpose()
            .map_err(|e| io_err(&e))?
            .map(|e| e.log_id);

        let last_purged = *self.last_purged.read().await;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NID>) -> Result<(), StorageError<NID>> {
        let meta = self.meta_tree();
        let data = serde_json::to_vec(vote).map_err(|e| io_err(&e))?;
        meta.insert("vote", data).map_err(|e| io_err(&e))?;
        meta.flush().map_err(|e| io_err(&e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NID>>, StorageError<NID>> {
        let meta = self.meta_tree();
        let vote = meta
            .get("vote")
            .map_err(|e| io_err(&e))?
            .map(|v| serde_json::from_slice(&v))
            .transpose()
            .map_err(|e| io_err(&e))?;
        Ok(vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NID>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let log = self.log_tree();
        let mut batch = sled::Batch::default();
        for entry in entries {
            let key = Self::index_key(entry.log_id.index);
            let val = serde_json::to_vec(&entry).map_err(|e| io_err(&e))?;
            batch.insert(&key, val);
        }
        log.apply_batch(batch).map_err(|e| io_err(&e))?;
        log.flush().map_err(|e| io_err(&e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NID>) -> Result<(), StorageError<NID>> {
        let log = self.log_tree();
        let start = Self::index_key(log_id.index);
        let keys: Vec<_> = log
            .range(start..)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        let mut batch = sled::Batch::default();
        for key in keys {
            batch.remove(key);
        }
        log.apply_batch(batch).map_err(|e| io_err(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NID>) -> Result<(), StorageError<NID>> {
        let log = self.log_tree();
        let end = Self::index_key(log_id.index);
        let keys: Vec<_> = log
            .range(..=end)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();
        let mut batch = sled::Batch::default();
        for key in keys {
            batch.remove(key);
        }
        log.apply_batch(batch).map_err(|e| io_err(&e))?;

        // Persist last_purged
        let meta = self.meta_tree();
        let data = serde_json::to_vec(&log_id).map_err(|e| io_err(&e))?;
        meta.insert("last_purged", data).map_err(|e| io_err(&e))?;
        *self.last_purged.write().await = Some(log_id);
        Ok(())
    }
}

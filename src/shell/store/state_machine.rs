use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use tokio::sync::RwLock;

use crate::core::types::{AppRequest, AppResponse};
use crate::TypeConfig;

type NID = u64;

fn io_err(e: impl std::fmt::Display) -> StorageError<NID> {
    StorageError::IO {
        source: StorageIOError::new(
            openraft::ErrorSubject::StateMachine,
            openraft::ErrorVerb::Write,
            anyerror::AnyError::error(e.to_string()),
        ),
    }
}

/// Sled-backed state machine. Three sled trees:
/// - `sm_entries` : index (u64 BE) → message (UTF-8 string)
/// - `sm_meta`    : "last_applied" → LogId (JSON), "last_membership" → StoredMembership (JSON)
/// - `sm_snap`    : "current" → snapshot bytes (JSON of BinlogState)
#[derive(Debug, Clone)]
pub struct StateMachineStore {
    db: Arc<sled::Db>,
    /// Entry count cache for fast index assignment on reads.
    entry_count: Arc<RwLock<u64>>,
}

/// Serializable snapshot of the entire state machine, for snapshot transport.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BinlogSnapshot {
    last_applied_log: Option<LogId<NID>>,
    last_membership: StoredMembership<NID, BasicNode>,
    entries: Vec<String>,
}

impl StateMachineStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path.as_ref().join("state-machine"))?;
        let entries_tree = db.open_tree("sm_entries")?;
        let count = entries_tree.len() as u64;

        Ok(Self {
            db: Arc::new(db),
            entry_count: Arc::new(RwLock::new(count)),
        })
    }

    fn entries_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_entries").expect("open sm_entries tree")
    }

    fn meta_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_meta").expect("open sm_meta tree")
    }

    fn snap_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_snap").expect("open sm_snap tree")
    }

    fn read_last_applied(&self) -> Result<Option<LogId<NID>>, StorageError<NID>> {
        let meta = self.meta_tree();
        meta.get("last_applied")
            .map_err(|e| io_err(&e))?
            .map(|v| serde_json::from_slice(&v))
            .transpose()
            .map_err(|e| io_err(&e))
    }

    fn read_last_membership(&self) -> Result<StoredMembership<NID, BasicNode>, StorageError<NID>> {
        let meta = self.meta_tree();
        meta.get("last_membership")
            .map_err(|e| io_err(&e))?
            .map(|v| serde_json::from_slice(&v))
            .transpose()
            .map_err(|e| io_err(&e))
            .map(|opt| opt.unwrap_or_default())
    }

    /// Read all committed entries (used by GET /api/log).
    pub async fn read_entries(&self) -> Result<Vec<String>, StorageError<NID>> {
        let tree = self.entries_tree();
        let mut entries = Vec::new();
        for item in tree.iter() {
            let (_, val) = item.map_err(|e| io_err(&e))?;
            let msg = String::from_utf8(val.to_vec()).map_err(|e| io_err(&e))?;
            entries.push(msg);
        }
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NID>> {
        let last_applied = self.read_last_applied()?;
        let last_membership = self.read_last_membership()?;
        let entries = self.read_entries().await?;

        let snap = BinlogSnapshot {
            last_applied_log: last_applied,
            last_membership: last_membership.clone(),
            entries,
        };

        let data = serde_json::to_vec(&snap).map_err(|e| io_err(&e))?;

        let snapshot_id = format!(
            "{}-{}",
            last_applied.map(|l| l.leader_id.term).unwrap_or(0),
            last_applied.map(|l| l.index).unwrap_or(0),
        );

        // Cache snapshot for get_current_snapshot
        self.snap_tree()
            .insert("current", data.clone())
            .map_err(|e| io_err(&e))?;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NID>>, StoredMembership<NID, BasicNode>), StorageError<NID>> {
        let last_applied = self.read_last_applied()?;
        let last_membership = self.read_last_membership()?;
        Ok((last_applied, last_membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<AppResponse>, StorageError<NID>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries_tree = self.entries_tree();
        let meta = self.meta_tree();
        let mut results = Vec::new();

        for entry in entries {
            // Persist last_applied
            let applied_data = serde_json::to_vec(&entry.log_id).map_err(|e| io_err(&e))?;
            meta.insert("last_applied", applied_data).map_err(|e| io_err(&e))?;

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message: String::new(),
                    });
                }
                EntryPayload::Normal(req) => {
                    let AppRequest::Append { message } = req;
                    // Append to entries tree with auto-incrementing key
                    let mut count = self.entry_count.write().await;
                    let key = count.to_be_bytes();
                    entries_tree
                        .insert(key, message.as_bytes())
                        .map_err(|e| io_err(&e))?;
                    *count += 1;
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message,
                    });
                }
                EntryPayload::Membership(mem) => {
                    let stored = StoredMembership::new(Some(entry.log_id), mem);
                    let data = serde_json::to_vec(&stored).map_err(|e| io_err(&e))?;
                    meta.insert("last_membership", data).map_err(|e| io_err(&e))?;
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message: String::new(),
                    });
                }
            }
        }

        entries_tree.flush().map_err(|e| io_err(&e))?;
        meta.flush().map_err(|e| io_err(&e))?;
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NID>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NID, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NID>> {
        let data = snapshot.into_inner();
        let snap: BinlogSnapshot =
            serde_json::from_slice(&data).map_err(|e| io_err(&e))?;

        // Clear and repopulate entries tree
        let entries_tree = self.entries_tree();
        entries_tree.clear().map_err(|e| io_err(&e))?;
        for (i, msg) in snap.entries.iter().enumerate() {
            let key = (i as u64).to_be_bytes();
            entries_tree.insert(key, msg.as_bytes()).map_err(|e| io_err(&e))?;
        }
        *self.entry_count.write().await = snap.entries.len() as u64;

        // Write meta
        let meta_tree = self.meta_tree();
        let applied = serde_json::to_vec(&meta.last_log_id).map_err(|e| io_err(&e))?;
        meta_tree.insert("last_applied", applied).map_err(|e| io_err(&e))?;
        let membership = serde_json::to_vec(&meta.last_membership).map_err(|e| io_err(&e))?;
        meta_tree.insert("last_membership", membership).map_err(|e| io_err(&e))?;

        // Cache snapshot
        self.snap_tree()
            .insert("current", data)
            .map_err(|e| io_err(&e))?;

        entries_tree.flush().map_err(|e| io_err(&e))?;
        meta_tree.flush().map_err(|e| io_err(&e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NID>> {
        let snap_data = self.snap_tree().get("current").map_err(|e| io_err(&e))?;
        match snap_data {
            None => Ok(None),
            Some(data) => {
                let snap: BinlogSnapshot =
                    serde_json::from_slice(&data).map_err(|e| io_err(&e))?;
                let snapshot_id = format!(
                    "{}-{}",
                    snap.last_applied_log.map(|l| l.leader_id.term).unwrap_or(0),
                    snap.last_applied_log.map(|l| l.index).unwrap_or(0),
                );
                Ok(Some(Snapshot {
                    meta: SnapshotMeta {
                        last_log_id: snap.last_applied_log,
                        last_membership: snap.last_membership,
                        snapshot_id,
                    },
                    snapshot: Box::new(Cursor::new(data.to_vec())),
                }))
            }
        }
    }
}

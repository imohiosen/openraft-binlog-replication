//! Sled-backed persistence adapter for the state machine.
//!
//! This is the **imperative shell** — it owns the sled database and persists
//! state changes. All business logic lives in `core::state_machine::BinlogState`.

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use tokio::sync::RwLock;

use crate::core::state_machine::BinlogState;
use crate::core::types::AppResponse;
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

/// Sled-backed state machine adapter.
///
/// Holds the pure `BinlogState` in memory (the functional core) and
/// persists every mutation to sled for durability.
#[derive(Debug, Clone)]
pub struct StateMachineStore {
    db: Arc<sled::Db>,
    /// The canonical in-memory state — all reads and writes go through here.
    state: Arc<RwLock<BinlogState>>,
}

impl StateMachineStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path.as_ref().join("state-machine"))?;
        let state = Self::recover_state(&db)?;
        Ok(Self {
            db: Arc::new(db),
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Rebuild `BinlogState` from sled trees on startup.
    fn recover_state(db: &sled::Db) -> Result<BinlogState, sled::Error> {
        let meta = db.open_tree("sm_meta")?;
        let entries_tree = db.open_tree("sm_entries")?;

        let last_applied_log = meta
            .get("last_applied")?
            .and_then(|v| serde_json::from_slice(&v).ok());

        let last_membership = meta
            .get("last_membership")?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or_default();

        let mut entries = Vec::new();
        for item in entries_tree.iter() {
            let (_, val) = item?;
            if let Ok(msg) = String::from_utf8(val.to_vec()) {
                entries.push(msg);
            }
        }

        Ok(BinlogState {
            last_applied_log,
            last_membership,
            entries,
        })
    }

    // ── Sled persistence helpers (pure IO, no logic) ─────────────────

    fn entries_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_entries").expect("open sm_entries tree")
    }

    fn meta_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_meta").expect("open sm_meta tree")
    }

    fn snap_tree(&self) -> sled::Tree {
        self.db.open_tree("sm_snap").expect("open sm_snap tree")
    }

    /// Persist a single appended entry to the sled entries tree.
    fn persist_entry(&self, index: u64, message: &str) -> Result<(), StorageError<NID>> {
        let key = index.to_be_bytes();
        self.entries_tree()
            .insert(key, message.as_bytes())
            .map_err(|e| io_err(&e))?;
        Ok(())
    }

    /// Persist last_applied log id to sled meta.
    fn persist_last_applied(&self, log_id: &LogId<NID>) -> Result<(), StorageError<NID>> {
        let data = serde_json::to_vec(log_id).map_err(|e| io_err(&e))?;
        self.meta_tree()
            .insert("last_applied", data)
            .map_err(|e| io_err(&e))?;
        Ok(())
    }

    /// Persist membership to sled meta.
    fn persist_membership(
        &self,
        membership: &StoredMembership<NID, BasicNode>,
    ) -> Result<(), StorageError<NID>> {
        let data = serde_json::to_vec(membership).map_err(|e| io_err(&e))?;
        self.meta_tree()
            .insert("last_membership", data)
            .map_err(|e| io_err(&e))?;
        Ok(())
    }

    /// Flush entries + meta trees.
    fn flush(&self) -> Result<(), StorageError<NID>> {
        self.entries_tree().flush().map_err(|e| io_err(&e))?;
        self.meta_tree().flush().map_err(|e| io_err(&e))?;
        Ok(())
    }

    /// Rebuild sled entries tree from in-memory state (used after snapshot install).
    fn rebuild_entries_from_state(&self, state: &BinlogState) -> Result<(), StorageError<NID>> {
        let entries_tree = self.entries_tree();
        entries_tree.clear().map_err(|e| io_err(&e))?;
        for (i, msg) in state.entries.iter().enumerate() {
            let key = (i as u64).to_be_bytes();
            entries_tree
                .insert(key, msg.as_bytes())
                .map_err(|e| io_err(&e))?;
        }
        Ok(())
    }

    // ── Public read API (delegates to core) ──────────────────────────

    /// Read all committed entries.
    pub async fn read_entries(&self) -> Result<Vec<String>, StorageError<NID>> {
        let state = self.state.read().await;
        Ok(state.entries().to_vec())
    }
}

// ── openraft trait implementations (thin shell around BinlogState) ────

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NID>> {
        let state = self.state.read().await;

        let data = state.to_snapshot_bytes().map_err(|e| io_err(&e))?;
        let snapshot_id = state.snapshot_id();
        let last_membership = state.last_membership.clone();
        let last_applied = state.last_applied_log;

        drop(state); // release lock before sled write

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
        let state = self.state.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<AppResponse>, StorageError<NID>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut state = self.state.write().await;
        let mut results = Vec::new();

        for entry in entries {
            // ── Core: pure state mutation ──
            state.set_last_applied(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => BinlogState::apply_blank(entry.log_id.index),
                EntryPayload::Normal(req) => {
                    let entry_index = state.entries.len() as u64;
                    let resp = state.apply_request(entry.log_id.index, req);
                    // ── Shell: persist the new entry to sled ──
                    self.persist_entry(entry_index, &resp.message)?;
                    resp
                }
                EntryPayload::Membership(mem) => {
                    let stored = StoredMembership::new(Some(entry.log_id), mem);
                    let resp = state.apply_membership(entry.log_id, stored.clone());
                    // ── Shell: persist membership to sled ──
                    self.persist_membership(&stored)?;
                    resp
                }
            };

            // ── Shell: persist last_applied to sled ──
            self.persist_last_applied(&entry.log_id)?;
            results.push(response);
        }

        self.flush()?;
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

        // ── Core: restore state from snapshot bytes ──
        let new_state =
            BinlogState::from_snapshot_bytes(&data).map_err(|e| io_err(&e))?;

        // ── Shell: rebuild sled from restored core state ──
        self.rebuild_entries_from_state(&new_state)?;

        let meta_tree = self.meta_tree();
        let applied = serde_json::to_vec(&meta.last_log_id).map_err(|e| io_err(&e))?;
        meta_tree
            .insert("last_applied", applied)
            .map_err(|e| io_err(&e))?;
        let membership = serde_json::to_vec(&meta.last_membership).map_err(|e| io_err(&e))?;
        meta_tree
            .insert("last_membership", membership)
            .map_err(|e| io_err(&e))?;

        self.snap_tree()
            .insert("current", data)
            .map_err(|e| io_err(&e))?;

        self.flush()?;

        // ── Core: swap in the restored state ──
        *self.state.write().await = new_state;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NID>> {
        let snap_data = self.snap_tree().get("current").map_err(|e| io_err(&e))?;
        match snap_data {
            None => Ok(None),
            Some(data) => {
                let restored =
                    BinlogState::from_snapshot_bytes(&data).map_err(|e| io_err(&e))?;
                let snapshot_id = restored.snapshot_id();
                Ok(Some(Snapshot {
                    meta: SnapshotMeta {
                        last_log_id: restored.last_applied_log,
                        last_membership: restored.last_membership,
                        snapshot_id,
                    },
                    snapshot: Box::new(Cursor::new(data.to_vec())),
                }))
            }
        }
    }
}

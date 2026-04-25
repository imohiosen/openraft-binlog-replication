use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StoredMembership};
use tokio::sync::RwLock;

use crate::core::types::{AppRequest, AppResponse};
use crate::TypeConfig;

type NID = u64;

/// The in-memory state: an append-only log of string messages.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BinlogState {
    pub last_applied_log: Option<LogId<NID>>,
    pub last_membership: StoredMembership<NID, BasicNode>,
    pub entries: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct StateMachineStore {
    pub state: Arc<RwLock<BinlogState>>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NID>> {
        let state = self.state.read().await;
        let data = serde_json::to_vec(&*state)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::read_state_machine(&e),
            })?;
        let last_applied = state.last_applied_log;
        let last_membership = state.last_membership.clone();
        let snapshot_id = format!(
            "{}-{}",
            last_applied.map(|l| l.leader_id.term).unwrap_or(0),
            last_applied.map(|l| l.index).unwrap_or(0),
        );

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
            state.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message: String::new(),
                    });
                }
                EntryPayload::Normal(req) => {
                    let AppRequest::Append { message } = req;
                    state.entries.push(message.clone());
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message,
                    });
                }
                EntryPayload::Membership(mem) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    results.push(AppResponse {
                        index: entry.log_id.index,
                        message: String::new(),
                    });
                }
            }
        }

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
        let new_state: BinlogState =
            serde_json::from_slice(&data).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::read_state_machine(&e),
            })?;

        let mut state = self.state.write().await;
        *state = new_state;
        state.last_applied_log = meta.last_log_id;
        state.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NID>> {
        Ok(None)
    }
}

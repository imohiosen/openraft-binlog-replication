pub mod core;
pub mod shell;

use std::io::Cursor;

use openraft::BasicNode;

use crate::core::types::{AppRequest, AppResponse};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
        NodeId = u64,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type NodeId = u64;
pub type BinlogRaft = openraft::Raft<TypeConfig>;

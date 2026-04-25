use std::sync::Arc;

use openraft::Config;

use crate::shell::store::state_machine::StateMachineStore;
use crate::{BinlogRaft, NodeId};

pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub advertise_addr: String,
    pub raft: Arc<BinlogRaft>,
    pub state_machine: StateMachineStore,
    pub config: Arc<Config>,
}

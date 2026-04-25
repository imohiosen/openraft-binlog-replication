use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, InstallSnapshotError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::TypeConfig;

type NID = u64;

pub struct Network {
    pub client: reqwest::Client,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NID, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: node.addr.clone(),
            client: self.client.clone(),
            target,
        }
    }
}

pub struct NetworkConnection {
    addr: String,
    client: reqwest::Client,
    target: NID,
}

impl NetworkConnection {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NID>, RPCError<NID, BasicNode, RaftError<NID>>> {
        let resp: Result<AppendEntriesResponse<NID>, RaftError<NID>> = self
            .client
            .post(self.url("/raft/append"))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NID>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NID>, RPCError<NID, BasicNode, RaftError<NID>>> {
        let resp: Result<VoteResponse<NID>, RaftError<NID>> = self
            .client
            .post(self.url("/raft/vote"))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NID>,
        RPCError<NID, BasicNode, RaftError<NID, InstallSnapshotError>>,
    > {
        let resp: Result<InstallSnapshotResponse<NID>, RaftError<NID, InstallSnapshotError>> = self
            .client
            .post(self.url("/raft/snapshot"))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

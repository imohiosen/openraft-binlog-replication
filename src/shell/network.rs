use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, InstallSnapshotError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::shell::grpc::pb::raft_service_client::RaftServiceClient;
use crate::shell::grpc::pb::RaftRequest;
use crate::TypeConfig;

type NID = u64;

pub struct Network;

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: NID, node: &BasicNode) -> Self::Network {
        GrpcConnection {
            addr: node.addr.clone(),
            target,
            client: None,
        }
    }
}

pub struct GrpcConnection {
    addr: String,
    target: NID,
    client: Option<RaftServiceClient<tonic::transport::Channel>>,
}

impl GrpcConnection {
    async fn get_client(
        &mut self,
    ) -> Result<&mut RaftServiceClient<tonic::transport::Channel>, NetworkError> {
        if self.client.is_none() {
            let endpoint = format!("http://{}", self.addr);
            let client = RaftServiceClient::connect(endpoint)
                .await
                .map_err(|e| NetworkError::new(&e))?;
            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }

    fn net_err(e: impl std::fmt::Display) -> NetworkError {
        NetworkError::new(&anyerror::AnyError::error(e.to_string()))
    }
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NID>, RPCError<NID, BasicNode, RaftError<NID>>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(Self::net_err(e)))?;
        let client = self.get_client().await.map_err(RPCError::Network)?;
        let reply = client
            .append_entries(RaftRequest { data })
            .await
            .map_err(|e| RPCError::Network(Self::net_err(e)))?
            .into_inner();
        let resp: Result<AppendEntriesResponse<NID>, RaftError<NID>> =
            serde_json::from_slice(&reply.data)
                .map_err(|e| RPCError::Network(Self::net_err(e)))?;
        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NID>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NID>, RPCError<NID, BasicNode, RaftError<NID>>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(Self::net_err(e)))?;
        let client = self.get_client().await.map_err(RPCError::Network)?;
        let reply = client
            .vote(RaftRequest { data })
            .await
            .map_err(|e| RPCError::Network(Self::net_err(e)))?
            .into_inner();
        let resp: Result<VoteResponse<NID>, RaftError<NID>> =
            serde_json::from_slice(&reply.data)
                .map_err(|e| RPCError::Network(Self::net_err(e)))?;
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
        let data = serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(Self::net_err(e)))?;
        let client = self.get_client().await.map_err(RPCError::Network)?;
        let reply = client
            .install_snapshot(RaftRequest { data })
            .await
            .map_err(|e| RPCError::Network(Self::net_err(e)))?
            .into_inner();
        let resp: Result<InstallSnapshotResponse<NID>, RaftError<NID, InstallSnapshotError>> =
            serde_json::from_slice(&reply.data)
                .map_err(|e| RPCError::Network(Self::net_err(e)))?;
        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

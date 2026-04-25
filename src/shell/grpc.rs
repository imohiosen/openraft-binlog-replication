//! gRPC server for Raft inter-node RPCs (vote, append_entries, install_snapshot).

use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::BinlogRaft;
use crate::TypeConfig;

pub mod pb {
    tonic::include_proto!("raft");
}

use pb::raft_service_server::{RaftService, RaftServiceServer};
use pb::{RaftReply, RaftRequest};

pub struct RaftGrpcService {
    pub raft: Arc<BinlogRaft>,
}

#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let req: openraft::raft::VoteRequest<u64> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let res = self.raft.vote(req).await;
        let data = serde_json::to_vec(&res)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let res = self.raft.append_entries(req).await;
        let data = serde_json::to_vec(&res)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let res = self.raft.install_snapshot(req).await;
        let data = serde_json::to_vec(&res)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }
}

pub fn make_server(raft: Arc<BinlogRaft>) -> RaftServiceServer<RaftGrpcService> {
    RaftServiceServer::new(RaftGrpcService { raft })
}

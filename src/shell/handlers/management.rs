use std::collections::{BTreeMap, BTreeSet};

use actix_web::{get, post, web::Data, web::Json, Responder};
use openraft::error::Infallible;
use openraft::{BasicNode, RaftMetrics};

use crate::shell::app::App;
use crate::NodeId;

#[derive(serde::Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: NodeId,
    pub addr: String,
}

#[post("/cluster/init")]
pub async fn init(app: Data<App>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    nodes.insert(app.id, BasicNode { addr: app.advertise_addr.clone() });
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

#[post("/cluster/add-learner")]
pub async fn add_learner(
    app: Data<App>,
    req: Json<AddLearnerRequest>,
) -> actix_web::Result<impl Responder> {
    let node = BasicNode { addr: req.addr.clone() };
    let res = app.raft.add_learner(req.node_id, node, true).await;
    Ok(Json(res))
}

#[post("/cluster/change-membership")]
pub async fn change_membership(
    app: Data<App>,
    req: Json<BTreeSet<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, false).await;
    Ok(Json(res))
}

#[get("/cluster/metrics")]
pub async fn metrics(app: Data<App>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();
    let res: Result<RaftMetrics<u64, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

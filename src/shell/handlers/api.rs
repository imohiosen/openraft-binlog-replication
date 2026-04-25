use actix_web::{get, post, web::Data, web::Json, Responder};

use crate::core::types::AppRequest;
use crate::shell::app::App;

#[derive(serde::Deserialize)]
pub struct AppendRequest {
    pub message: String,
}

#[derive(serde::Serialize)]
pub struct LogResponse {
    pub entries: Vec<String>,
}

#[derive(serde::Serialize)]
pub struct LeaderResponse {
    pub leader_id: Option<u64>,
    pub current_node: u64,
    pub state: String,
}

/// Append a message to the replicated log (writes go through Raft).
#[post("/api/append")]
pub async fn append_entry(
    app: Data<App>,
    req: Json<AppendRequest>,
) -> actix_web::Result<impl Responder> {
    let request = AppRequest::Append {
        message: req.message.clone(),
    };
    let res = app.raft.client_write(request).await;
    Ok(Json(res))
}

/// Read the current committed log from this node's state machine.
#[get("/api/log")]
pub async fn read_log(app: Data<App>) -> actix_web::Result<impl Responder> {
    let state = app.state_machine.state.read().await;
    Ok(Json(LogResponse {
        entries: state.entries.clone(),
    }))
}

/// Report the current leader (useful for demo/debugging).
#[get("/api/leader")]
pub async fn leader(app: Data<App>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();
    Ok(Json(LeaderResponse {
        leader_id: metrics.current_leader,
        current_node: app.id,
        state: format!("{:?}", metrics.state),
    }))
}

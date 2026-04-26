use actix_web::{get, post, web::Data, web::Json, HttpResponse, Responder};

use crate::core::sql::types::{Row, SqlResult, Value};
use crate::core::types::AppRequest;
use crate::shell::app::App;
use crate::shell::sql::parser::{parse_sql, ParsedStatement};

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

#[derive(serde::Deserialize)]
pub struct SqlRequest {
    pub sql: String,
}

#[derive(serde::Serialize)]
pub struct SqlResponse {
    pub results: Vec<SqlResultJson>,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
pub enum SqlResultJson {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Ack {
        rows_affected: u64,
    },
    Status {
        status: String,
    },
    Error {
        error: String,
    },
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
    let entries = app
        .state_machine
        .read_entries()
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(format!("{}", e)))?;
    Ok(Json(LogResponse { entries }))
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

/// Execute one or more SQL statements.
#[post("/api/sql")]
pub async fn execute_sql(
    app: Data<App>,
    req: Json<SqlRequest>,
) -> actix_web::Result<HttpResponse> {
    let stmts = match parse_sql(&req.sql) {
        Ok(s) => s,
        Err(e) => {
            return Ok(HttpResponse::BadRequest().json(SqlResponse {
                results: vec![SqlResultJson::Error {
                    error: e.to_string(),
                }],
            }));
        }
    };

    let mut results = Vec::new();

    for stmt in stmts {
        match stmt {
            ParsedStatement::Query(plan) => {
                // Linearizable read: confirm leadership before serving.
                // Only the leader can serve linearizable reads. Followers
                // return the leader hint so clients can redirect.
                if let Err(e) = app.raft.ensure_linearizable().await {
                    let metrics = app.raft.metrics().borrow().clone();
                    let leader_hint = metrics.current_leader;
                    results.push(SqlResultJson::Error {
                        error: format!(
                            "not leader; forward SELECT to leader node {} (linearizable read requires leader)",
                            leader_hint.map(|id| id.to_string()).unwrap_or("unknown".into())
                        ),
                    });
                    continue;
                }

                match app.state_machine.query_select(&plan).await {
                    Ok(sql_result) => results.push(sql_result_to_json(sql_result)),
                    Err(e) => results.push(SqlResultJson::Error { error: e }),
                }
            }
            ParsedStatement::Catalog(query) => {
                // Catalog queries (SHOW/DESCRIBE) use linearizable reads.
                if let Err(e) = app.raft.ensure_linearizable().await {
                    let metrics = app.raft.metrics().borrow().clone();
                    let leader_hint = metrics.current_leader;
                    results.push(SqlResultJson::Error {
                        error: format!(
                            "not leader; forward catalog query to leader node {} (linearizable read requires leader)",
                            leader_hint.map(|id| id.to_string()).unwrap_or("unknown".into())
                        ),
                    });
                    continue;
                }

                match app.state_machine.query_catalog(&query).await {
                    Ok(sql_result) => results.push(sql_result_to_json(sql_result)),
                    Err(e) => results.push(SqlResultJson::Error { error: e }),
                }
            }
            ParsedStatement::Command(cmd) => {
                let request = AppRequest::Sql(cmd);
                match app.raft.client_write(request).await {
                    Ok(resp) => {
                        if let crate::core::types::AppResponse::Sql { result, .. } = resp.data {
                            results.push(sql_result_to_json(result));
                        } else {
                            results.push(SqlResultJson::Status {
                                status: "ok".to_string(),
                            });
                        }
                    }
                    Err(e) => {
                        results.push(SqlResultJson::Error {
                            error: format!("{}", e),
                        });
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().json(SqlResponse { results }))
}

fn sql_result_to_json(result: SqlResult) -> SqlResultJson {
    match result {
        SqlResult::Rows { columns, rows } => SqlResultJson::Rows {
            columns,
            rows: rows.into_iter().map(row_to_json).collect(),
        },
        SqlResult::Ack { rows_affected } => SqlResultJson::Ack { rows_affected },
        SqlResult::Created => SqlResultJson::Status {
            status: "created".to_string(),
        },
        SqlResult::Dropped => SqlResultJson::Status {
            status: "dropped".to_string(),
        },
        SqlResult::Truncated => SqlResultJson::Status {
            status: "truncated".to_string(),
        },
        SqlResult::Error(e) => SqlResultJson::Error { error: e },
    }
}

fn row_to_json(row: Row) -> Vec<serde_json::Value> {
    row.into_iter().map(value_to_json).collect()
}

fn value_to_json(val: Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Int(n) => serde_json::json!(n),
        Value::Text(s) => serde_json::json!(s),
        Value::Bool(b) => serde_json::json!(b),
        Value::Real(f) => serde_json::json!(f),
    }
}

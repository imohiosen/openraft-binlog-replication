use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, HttpServer};

use crate::shell::app::App;
use crate::shell::handlers::{api, management, raft};

pub async fn run(app: Data<App>, bind_addr: &str) -> std::io::Result<()> {
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            // Raft internal RPCs
            .service(raft::vote)
            .service(raft::append)
            .service(raft::snapshot)
            // Cluster management
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // Application API
            .service(api::append_entry)
            .service(api::read_log)
            .service(api::leader)
    });

    server.bind(bind_addr)?.run().await
}

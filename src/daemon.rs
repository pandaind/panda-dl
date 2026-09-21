use anyhow::Result;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::capture::ClipboardWatcher;
use crate::config::Config;
use crate::engine::Engine;
use crate::ipc::{IpcServer, Request};

pub async fn run_daemon() -> Result<()> {
    info!("Starting Panda-DL background daemon...");

    let config = Arc::new(RwLock::new(Config::load()));
    let engine = Engine::new(Arc::clone(&config)).await?;

    let watcher = Arc::new(ClipboardWatcher::new(
        config.read().await.auto_capture_clipboard,
    ));

    // Spawn clipboard watcher
    let engine_for_clip = Arc::clone(&engine);
    let config_for_clip = Arc::clone(&config);
    watcher
        .start_listener(move |text, _kind| {
            let engine = Arc::clone(&engine_for_clip);
            let config = Arc::clone(&config_for_clip);
            async move {
                let req = Request::ClipboardEvent { text };
                let _ = IpcServer::handle_request(req, &engine, &config).await;
            }
        })
        .await;

    // Start IPC server
    let server = IpcServer::new(Arc::clone(&engine), Arc::clone(&config));

    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                error!("IPC Server exited with error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down gracefully...");
        }
    }

    let _ = engine.save_tasks().await;
    let socket_path = IpcServer::socket_path();
    if socket_path.exists() {
        let _ = std::fs::remove_file(socket_path);
    }
    info!("Panda-DL daemon shut down cleanly.");
    Ok(())
}

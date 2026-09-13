use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::engine::types::DaemonStatus;
use crate::engine::Engine;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Request {
    Status,
    Add {
        uri: String,
        #[serde(default)]
        dir: Option<String>,
        #[serde(default)]
        parts: Option<usize>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Remove {
        id: String,
        #[serde(default)]
        delete_file: bool,
    },
    OpenFile {
        id: String,
    },
    OpenFolder {
        id: String,
    },
    GetConfig,
    SetConfig {
        config: Config,
    },
    ClipboardEvent {
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Status(DaemonStatus),
    Config(Config),
    Error {
        message: String,
    },
}

pub struct IpcServer {
    engine: Arc<Engine>,
    config: Arc<RwLock<Config>>,
}

impl IpcServer {
    pub fn new(engine: Arc<Engine>, config: Arc<RwLock<Config>>) -> Self {
        Self { engine, config }
    }

    pub fn socket_path() -> PathBuf {
        let base = dirs::data_dir().unwrap_or_else(|| {
            dirs::home_dir()
                .map(|h| h.join(".local/share"))
                .unwrap_or_else(|| PathBuf::from("."))
        });
        base.join("panda-dl").join("panda-dl.sock")
    }

    pub async fn run(&self) -> Result<()> {
        let path = Self::socket_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Remove old socket if exists
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }

        let listener = UnixListener::bind(&path)
            .context(format!("Failed to bind unix socket at {}", path.display()))?;

        info!("IPC server listening on {}", path.display());

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let engine = Arc::clone(&self.engine);
                    let config = Arc::clone(&self.config);
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, engine, config).await {
                            warn!("Error handling IPC connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Socket accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        stream: UnixStream,
        engine: Arc<Engine>,
        config: Arc<RwLock<Config>>,
    ) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        while let Some(line) = lines.next_line().await? {
            let req: Request = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    let err_resp = Response::Error {
                        message: format!("Invalid JSON request: {}", e),
                    };
                    let out = serde_json::to_string(&err_resp)? + "\n";
                    writer.write_all(out.as_bytes()).await?;
                    continue;
                }
            };

            let resp = Self::handle_request(req, &engine, &config).await;
            let out = serde_json::to_string(&resp)? + "\n";
            writer.write_all(out.as_bytes()).await?;
        }

        Ok(())
    }

    pub async fn handle_request(
        req: Request,
        engine: &Arc<Engine>,
        config: &Arc<RwLock<Config>>,
    ) -> Response {
        match req {
            Request::Status => {
                let status = engine.get_status().await;
                Response::Status(status)
            }
            Request::Add { uri, dir, parts } => {
                let custom_dir = dir.map(PathBuf::from);
                match engine.add_download(&uri, custom_dir, parts).await {
                    Ok(id) => Response::Ok {
                        id: Some(id),
                        message: Some("Download added successfully".to_string()),
                    },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::Pause { id } => match engine.pause_download(&id).await {
                Ok(()) => Response::Ok {
                    id: Some(id),
                    message: Some("Download paused".to_string()),
                },
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            Request::Resume { id } => match engine.resume_download(&id).await {
                Ok(()) => Response::Ok {
                    id: Some(id),
                    message: Some("Download resumed".to_string()),
                },
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            Request::Remove { id, delete_file } => {
                match engine.remove_download(&id, delete_file).await {
                    Ok(()) => Response::Ok {
                        id: Some(id),
                        message: Some("Download removed".to_string()),
                    },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::OpenFile { id } => {
                let status = engine.get_status().await;
                if let Some(task) = status.downloads.iter().find(|t| t.id == id) {
                    let path = task.output_path.clone();
                    tokio::spawn(async move {
                        let _ = tokio::process::Command::new("xdg-open")
                            .arg(&path)
                            .status()
                            .await;
                    });
                    Response::Ok {
                        id: Some(id),
                        message: Some("Opened file".to_string()),
                    }
                } else {
                    Response::Error {
                        message: "Task not found".to_string(),
                    }
                }
            }
            Request::OpenFolder { id } => {
                let status = engine.get_status().await;
                if let Some(task) = status.downloads.iter().find(|t| t.id == id) {
                    let path = PathBuf::from(&task.output_path);
                    let folder = if path.is_dir() {
                        path
                    } else {
                        path.parent().map(|p| p.to_path_buf()).unwrap_or(path)
                    };
                    tokio::spawn(async move {
                        let _ = tokio::process::Command::new("xdg-open")
                            .arg(&folder)
                            .status()
                            .await;
                    });
                    Response::Ok {
                        id: Some(id),
                        message: Some("Opened folder".to_string()),
                    }
                } else {
                    Response::Error {
                        message: "Task not found".to_string(),
                    }
                }
            }
            Request::GetConfig => {
                let cfg = config.read().await.clone();
                Response::Config(cfg)
            }
            Request::SetConfig { config: new_cfg } => {
                if let Err(e) = new_cfg.save() {
                    return Response::Error {
                        message: format!("Failed to save config: {}", e),
                    };
                }
                let mut cfg = config.write().await;
                *cfg = new_cfg.clone();
                Response::Config(new_cfg)
            }
            Request::ClipboardEvent { text } => {
                let cfg = config.read().await.clone();
                if !cfg.auto_capture_clipboard {
                    return Response::Ok {
                        id: None,
                        message: Some("Auto-capture disabled".to_string()),
                    };
                }

                let trimmed = text.trim();
                let is_magnet = trimmed.starts_with("magnet:?");
                let is_torrent = trimmed.ends_with(".torrent");

                if (is_magnet && cfg.auto_start_magnets) || (is_torrent && cfg.auto_start_torrents) {
                    match engine.add_download(trimmed, None, None).await {
                        Ok(id) => {
                            crate::capture::send_notification(
                                "Panda Downloader",
                                &format!("Captured & started downloading: {}", if is_magnet { "Magnet link" } else { "Torrent" }),
                            );
                            Response::Ok {
                                id: Some(id),
                                message: Some("Auto-started download from clipboard".to_string()),
                            }
                        }
                        Err(e) => Response::Error {
                            message: format!("Failed to auto-start download: {}", e),
                        },
                    }
                } else {
                    Response::Ok {
                        id: None,
                        message: Some("Clipboard event checked".to_string()),
                    }
                }
            }
        }
    }
}

pub struct IpcClient;

impl IpcClient {
    pub async fn send(req: Request) -> Result<Response> {
        let path = IpcServer::socket_path();
        if !path.exists() {
            bail!("Panda-DL daemon is not running (socket not found at {})", path.display());
        }

        let stream = UnixStream::connect(&path)
            .await
            .context("Failed to connect to Panda-DL socket")?;

        let (reader, mut writer) = stream.into_split();
        let payload = serde_json::to_string(&req)? + "\n";
        writer.write_all(payload.as_bytes()).await?;

        let mut lines = BufReader::new(reader).lines();
        if let Some(line) = lines.next_line().await? {
            let resp: Response = serde_json::from_str(&line)?;
            Ok(resp)
        } else {
            bail!("Connection closed without response");
        }
    }
}

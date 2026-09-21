pub mod http;
pub mod monitor;
pub mod storage;
pub mod torrent;
pub mod types;
pub mod youtube;

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::capture::send_notification;
use crate::config::Config;
use http::HttpDownloader;
use torrent::TorrentManager;
use types::{DaemonStatus, DownloadKind, DownloadStatus, DownloadTask, PartInfo, TorrentInfo};
use youtube::YoutubeManager;

// ── Active-download tracking structs ────────────────────────────────────────

/// State kept for each in-flight HTTP download.
pub(super) struct HttpActive {
    pub cancel_token: CancellationToken,
    pub parts: Arc<Mutex<Vec<PartInfo>>>,
    #[allow(dead_code)]
    pub downloaded_atomic: Arc<AtomicU64>,
}

/// State kept for each in-flight YouTube download.
pub struct YoutubeActive {
    pub cancel_token: CancellationToken,
    pub downloaded_atomic: Arc<AtomicU64>,
}

// ── Engine ───────────────────────────────────────────────────────────────────

/// Central download engine. Coordinates HTTP, torrent, and YouTube downloads.
///
/// All mutable state is wrapped in `Arc<RwLock<…>>` / `Arc<Mutex<…>>` so the
/// engine can be shared freely across async tasks.
pub struct Engine {
    pub(super) config: Arc<RwLock<Config>>,
    pub(super) http: Arc<HttpDownloader>,
    pub(super) torrent: Arc<TorrentManager>,
    pub(super) youtube: Arc<YoutubeManager>,
    /// All known download tasks (active, paused, and completed).
    pub(super) tasks: Arc<RwLock<HashMap<String, DownloadTask>>>,
    pub(super) http_active: Arc<Mutex<HashMap<String, HttpActive>>>,
    pub(super) torrent_active: Arc<Mutex<HashMap<String, Arc<librqbit::ManagedTorrent>>>>,
    pub(super) youtube_active: Arc<Mutex<HashMap<String, YoutubeActive>>>,
    /// Per-task speed samples: `(downloaded_bytes, uploaded_bytes, sample_time)`.
    pub(super) last_speeds: Arc<Mutex<HashMap<String, (u64, u64, Instant)>>>,
}

impl Engine {
    /// Create a new engine, restore persisted tasks, and start the background monitor.
    pub async fn new(config: Arc<RwLock<Config>>) -> Result<Arc<Self>> {
        let download_dir = {
            let cfg = config.read().await;
            cfg.download_dir.clone()
        };

        let torrent = Arc::new(TorrentManager::new(download_dir).await?);
        let http = Arc::new(HttpDownloader::new());
        let youtube = Arc::new(YoutubeManager::new());
        let saved_tasks = storage::load_tasks();

        let engine = Arc::new(Self {
            config,
            http,
            torrent,
            youtube,
            tasks: Arc::new(RwLock::new(saved_tasks)),
            http_active: Arc::new(Mutex::new(HashMap::new())),
            torrent_active: Arc::new(Mutex::new(HashMap::new())),
            youtube_active: Arc::new(Mutex::new(HashMap::new())),
            last_speeds: Arc::new(Mutex::new(HashMap::new())),
        });

        // Spawn background monitor (ticks every 1 s) — defined in monitor.rs
        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            engine_clone.monitor_loop().await;
        });

        Ok(engine)
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    /// Flush all task state to disk (delegates to `storage::save_tasks`).
    pub async fn save_tasks(&self) -> Result<()> {
        let map = self.tasks.read().await;
        storage::save_tasks(&*map)
    }

    // ── Public API ───────────────────────────────────────────────────────────

    pub async fn add_download(
        self: &Arc<Self>,
        uri: &str,
        custom_dir: Option<PathBuf>,
        parts_count: Option<usize>,
    ) -> Result<String> {
        let uri = uri.trim();
        let is_youtube = uri.contains("youtube.com/watch") || uri.contains("youtu.be/");
        let is_torrent = !is_youtube
            && (uri.starts_with("magnet:?")
                || uri.ends_with(".torrent")
                || Path::new(uri)
                    .extension()
                    .map(|e| e == "torrent")
                    .unwrap_or(false));

        let default_dir = {
            let cfg = self.config.read().await;
            cfg.download_dir.clone()
        };
        let target_dir = custom_dir.unwrap_or(default_dir);
        std::fs::create_dir_all(&target_dir)?;

        let id = uuid::Uuid::new_v4().to_string();

        if is_youtube {
            self.start_youtube_download(id.clone(), uri.to_string(), target_dir)
                .await;
            return Ok(id);
        }

        if is_torrent {
            self.start_torrent_download(id.clone(), uri.to_string(), target_dir)
                .await;
            return Ok(id);
        }

        // HTTP / HTTPS download
        self.start_http_download_new(id.clone(), uri, target_dir, parts_count)
            .await?;
        Ok(id)
    }

    pub async fn pause_download(&self, id: &str) -> Result<()> {
        let kind = {
            let mut tasks = self.tasks.write().await;
            let task = tasks.get_mut(id).ok_or_else(|| anyhow::anyhow!("Task not found"))?;
            task.status = DownloadStatus::Paused;
            task.download_speed = 0;
            task.kind.clone()
        };

        match kind {
            DownloadKind::Http => {
                if let Some(active) = self.http_active.lock().await.remove(id) {
                    active.cancel_token.cancel();
                }
            }
            DownloadKind::Youtube => {
                if let Some(active) = self.youtube_active.lock().await.remove(id) {
                    active.cancel_token.cancel();
                }
            }
            DownloadKind::Torrent => {
                let handle = self.torrent_active.lock().await.get(id).cloned();
                if let Some(handle) = handle {
                    let _ = self.torrent.pause(&handle).await;
                }
            }
        }

        self.save_tasks().await?;
        Ok(())
    }

    pub async fn resume_download(self: &Arc<Self>, id: &str) -> Result<()> {
        let (kind, uri, output_path, parts) = {
            let mut tasks = self.tasks.write().await;
            let task = tasks.get_mut(id).ok_or_else(|| anyhow::anyhow!("Task not found"))?;

            if task.status != DownloadStatus::Paused
                && !matches!(task.status, DownloadStatus::Error(_))
            {
                return Ok(());
            }

            task.status = DownloadStatus::Downloading;
            (
                task.kind.clone(),
                task.uri.clone(),
                PathBuf::from(&task.output_path),
                task.parts.clone(),
            )
        };

        match kind {
            DownloadKind::Http => {
                let supports_ranges = parts.len() > 1;
                self.launch_http_download(id, &uri, &output_path, parts, supports_ranges)
                    .await;
            }
            DownloadKind::Youtube => {
                self.resume_youtube_download(id.to_string(), uri, output_path)
                    .await;
            }
            DownloadKind::Torrent => {
                let handle = self.torrent_active.lock().await.get(id).cloned();
                if let Some(handle) = handle {
                    let _ = self.torrent.unpause(&handle).await;
                } else {
                    let handle = self.torrent.add(&uri, Some(&output_path)).await?;
                    self.torrent_active
                        .lock()
                        .await
                        .insert(id.to_string(), handle);
                }
            }
        }

        self.save_tasks().await?;
        Ok(())
    }

    pub async fn remove_download(&self, id: &str, delete_file: bool) -> Result<()> {
        // Cancel any in-flight download
        if let Some(active) = self.http_active.lock().await.remove(id) {
            active.cancel_token.cancel();
        }
        if let Some(active) = self.youtube_active.lock().await.remove(id) {
            active.cancel_token.cancel();
        }

        let task = {
            let mut tasks = self.tasks.write().await;
            tasks.remove(id)
        };

        if let Some(task) = task {
            let handle = self.torrent_active.lock().await.remove(id);
            if let Some(handle) = handle {
                let _ = self.torrent.remove(&handle, delete_file).await;
            }

            if delete_file {
                let p = Path::new(&task.output_path);
                if p.is_file() {
                    let _ = std::fs::remove_file(p);
                } else if p.is_dir() {
                    let _ = std::fs::remove_dir_all(p);
                }
            }
        }

        self.save_tasks().await?;
        Ok(())
    }

    pub async fn get_status(&self) -> DaemonStatus {
        let tasks = self.tasks.read().await;
        let mut list: Vec<DownloadTask> = tasks.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        let active_count = list
            .iter()
            .filter(|t| t.status == DownloadStatus::Downloading)
            .count();
        let total_down: u64 = list.iter().map(|t| t.download_speed).sum();
        let total_up: u64 = list.iter().map(|t| t.upload_speed).sum();

        DaemonStatus {
            active_downloads: active_count,
            total_download_speed: total_down,
            total_upload_speed: total_up,
            downloads: list,
        }
    }

    // ── Private download launchers ───────────────────────────────────────────

    async fn start_youtube_download(
        self: &Arc<Self>,
        id: String,
        uri: String,
        target_dir: PathBuf,
    ) {
        let engine = self.clone();
        tokio::spawn(async move {
            let downloaded_atomic = Arc::new(AtomicU64::new(0));
            let cancel_token = CancellationToken::new();

            let task = DownloadTask {
                id: id.clone(),
                name: "YouTube Video".to_string(),
                uri: uri.clone(),
                kind: DownloadKind::Youtube,
                status: DownloadStatus::Queued,
                total_bytes: None,
                downloaded_bytes: 0,
                uploaded_bytes: 0,
                download_speed: 0,
                upload_speed: 0,
                eta_seconds: None,
                output_path: target_dir.to_string_lossy().into_owned(),
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                parts: vec![],
                torrent_info: None,
            };

            engine.tasks.write().await.insert(id.clone(), task);
            let _ = engine.save_tasks().await;

            // Resolve title before starting the actual download
            let mut display_name = "YouTube Video".to_string();
            if let Some(title) = youtube::YoutubeManager::get_title(&uri).await {
                display_name = title.clone();
                if let Some(t) = engine.tasks.write().await.get_mut(&id) {
                    t.name = title;
                }
            }

            if let Some(t) = engine.tasks.write().await.get_mut(&id) {
                t.status = DownloadStatus::Downloading;
            }

            let active = YoutubeActive {
                cancel_token: cancel_token.clone(),
                downloaded_atomic: downloaded_atomic.clone(),
            };
            engine.youtube_active.lock().await.insert(id.clone(), active);

            let res = engine
                .youtube
                .download(&uri, &target_dir, downloaded_atomic.clone(), cancel_token.clone())
                .await;

            engine.youtube_active.lock().await.remove(&id);

            let mut tasks = engine.tasks.write().await;
            if let Some(t) = tasks.get_mut(&id) {
                if cancel_token.is_cancelled() {
                    t.status = DownloadStatus::Paused;
                } else if let Err(ref e) = res {
                    t.status = DownloadStatus::Error(e.to_string());
                } else {
                    t.status = DownloadStatus::Completed;
                    t.downloaded_bytes =
                        downloaded_atomic.load(std::sync::atomic::Ordering::Relaxed);
                }
            }
            drop(tasks);
            let _ = engine.save_tasks().await;

            if res.is_ok() && !cancel_token.is_cancelled() {
                send_notification("YouTube Download Complete", &display_name);
            }
        });
    }

    async fn start_torrent_download(
        self: &Arc<Self>,
        id: String,
        uri: String,
        target_dir: PathBuf,
    ) {
        let initial_name = if uri.starts_with("magnet:?") {
            uri.split("&dn=")
                .nth(1)
                .and_then(|s| s.split('&').next())
                .map(|s| urlencoding::decode(s).unwrap_or_else(|_| s.into()).into_owned())
                .unwrap_or_else(|| "Magnet Download".to_string())
        } else {
            Path::new(&uri)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("Torrent Download")
                .to_string()
        };

        let task = DownloadTask {
            id: id.clone(),
            name: initial_name,
            uri: uri.clone(),
            kind: DownloadKind::Torrent,
            status: DownloadStatus::Downloading,
            total_bytes: None,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            download_speed: 0,
            upload_speed: 0,
            eta_seconds: None,
            output_path: target_dir.display().to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            parts: Vec::new(),
            torrent_info: None,
        };

        self.tasks.write().await.insert(id.clone(), task);
        let _ = self.save_tasks().await;

        let engine = Arc::clone(self);
        tokio::spawn(async move {
            match engine.torrent.add(&uri, Some(&target_dir)).await {
                Ok(handle) => {
                    let hash_hex = handle
                        .info_hash()
                        .0
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();

                    let mut tasks = engine.tasks.write().await;
                    if let Some(t) = tasks.get_mut(&id) {
                        if let Some(name) = handle.name() {
                            t.name = name;
                        }
                        t.torrent_info = Some(TorrentInfo {
                            info_hash: hash_hex,
                            seeds: 0,
                            peers: 0,
                            total_pieces: 0,
                            finished_pieces: 0,
                        });
                    }
                    drop(tasks);

                    engine.torrent_active.lock().await.insert(id, handle);
                    let _ = engine.save_tasks().await;
                    info!("Torrent added and active: {}", uri);
                }
                Err(e) => {
                    error!("Failed to resolve/add torrent: {}", e);
                    let mut tasks = engine.tasks.write().await;
                    if let Some(t) = tasks.get_mut(&id) {
                        t.status = DownloadStatus::Error(e.to_string());
                    }
                    drop(tasks);
                    let _ = engine.save_tasks().await;
                }
            }
        });
    }

    async fn start_http_download_new(
        self: &Arc<Self>,
        id: String,
        uri: &str,
        target_dir: PathBuf,
        parts_count: Option<usize>,
    ) -> Result<()> {
        let probe = self.http.probe(uri).await?;
        let output_path = HttpDownloader::resolve_unique_path(&target_dir, &probe.filename);

        let default_parts = {
            let cfg = self.config.read().await;
            cfg.default_parts
        };
        let num_parts = parts_count.unwrap_or(default_parts);

        let parts = if probe.supports_ranges
            && probe.total_bytes.map(|b| b > 1024 * 1024).unwrap_or(false)
        {
            HttpDownloader::create_parts(probe.total_bytes.unwrap(), num_parts)
        } else {
            vec![PartInfo {
                index: 0,
                start: 0,
                end: probe.total_bytes.unwrap_or(0),
                downloaded: 0,
                active: false,
            }]
        };

        // Pre-allocate the output file when the server supports byte-range requests
        if probe.supports_ranges {
            if let Some(total) = probe.total_bytes {
                let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&output_path)?;
                f.set_len(total)?;
            }
        }

        let task = DownloadTask {
            id: id.clone(),
            name: probe.filename,
            uri: uri.to_string(),
            kind: DownloadKind::Http,
            status: DownloadStatus::Downloading,
            total_bytes: probe.total_bytes,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            download_speed: 0,
            upload_speed: 0,
            eta_seconds: None,
            output_path: output_path.display().to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            parts: parts.clone(),
            torrent_info: None,
        };

        self.tasks.write().await.insert(id.clone(), task);
        let _ = self.save_tasks().await;

        self.launch_http_download(&id, uri, &output_path, parts, probe.supports_ranges)
            .await;
        Ok(())
    }

    /// Shared HTTP download launcher used by both new downloads and resume.
    async fn launch_http_download(
        self: &Arc<Self>,
        id: &str,
        uri: &str,
        output_path: &Path,
        parts: Vec<PartInfo>,
        supports_ranges: bool,
    ) {
        let cancel_token = CancellationToken::new();
        let parts_shared = Arc::new(Mutex::new(parts));
        let downloaded_atomic = Arc::new(AtomicU64::new(0));

        let active = HttpActive {
            cancel_token: cancel_token.clone(),
            parts: Arc::clone(&parts_shared),
            downloaded_atomic: Arc::clone(&downloaded_atomic),
        };
        self.http_active.lock().await.insert(id.to_string(), active);

        // Retain a second Arc so we can read final part state after the task finishes
        let parts_reader = Arc::clone(&parts_shared);

        let engine = Arc::clone(self);
        let id_owned = id.to_string();
        let uri_owned = uri.to_string();
        let output_path_owned = output_path.to_path_buf();

        tokio::spawn(async move {
            let res = if supports_ranges {
                engine
                    .http
                    .download_parts(
                        &uri_owned,
                        &output_path_owned,
                        parts_shared,
                        downloaded_atomic,
                        cancel_token.clone(),
                    )
                    .await
            } else {
                engine
                    .http
                    .download_single(
                        &uri_owned,
                        &output_path_owned,
                        downloaded_atomic,
                        cancel_token.clone(),
                    )
                    .await
            };

            let final_parts: Vec<PartInfo> = parts_reader.lock().await.clone();
            engine.http_active.lock().await.remove(&id_owned);

            let mut tasks = engine.tasks.write().await;
            if let Some(task) = tasks.get_mut(&id_owned) {
                if cancel_token.is_cancelled() {
                    // Preserve progress so resume can continue from the same offset
                    task.parts = final_parts;
                    task.status = DownloadStatus::Paused;
                } else if let Err(e) = res {
                    task.parts = final_parts;
                    error!("Download failed for {}: {}", task.name, e);
                    task.status = DownloadStatus::Error(e.to_string());
                } else {
                    // Mark all chunks complete
                    for part in &mut task.parts {
                        part.downloaded = part.end - part.start + 1;
                        part.active = false;
                    }
                    task.status = DownloadStatus::Completed;
                    if let Some(tot) = task.total_bytes {
                        task.downloaded_bytes = tot;
                    }
                    task.download_speed = 0;
                    task.eta_seconds = Some(0);
                    info!("Download complete: {}", task.name);
                    send_notification(
                        "Download Completed",
                        &format!("{} finished downloading!", task.name),
                    );
                }
            }
            drop(tasks);
            let _ = engine.save_tasks().await;
        });
    }

    async fn resume_youtube_download(
        self: &Arc<Self>,
        id: String,
        uri: String,
        target_dir: PathBuf,
    ) {
        let downloaded_atomic = Arc::new(AtomicU64::new(0));
        let cancel_token = CancellationToken::new();

        let active = YoutubeActive {
            cancel_token: cancel_token.clone(),
            downloaded_atomic: downloaded_atomic.clone(),
        };
        self.youtube_active.lock().await.insert(id.clone(), active);

        let engine = self.clone();
        tokio::spawn(async move {
            let res = engine
                .youtube
                .download(&uri, &target_dir, downloaded_atomic.clone(), cancel_token.clone())
                .await;
            engine.youtube_active.lock().await.remove(&id);
            let mut tasks = engine.tasks.write().await;
            if let Some(t) = tasks.get_mut(&id) {
                if cancel_token.is_cancelled() {
                    t.status = DownloadStatus::Paused;
                } else if let Err(ref e) = res {
                    t.status = DownloadStatus::Error(e.to_string());
                } else {
                    t.status = DownloadStatus::Completed;
                    t.downloaded_bytes =
                        downloaded_atomic.load(std::sync::atomic::Ordering::Relaxed);
                }
            }
            drop(tasks);
            let _ = engine.save_tasks().await;
        });
    }
}

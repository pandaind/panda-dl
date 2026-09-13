pub mod http;
pub mod torrent;
pub mod youtube;
pub mod types;

use anyhow::{bail, Result};
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
use youtube::YoutubeManager;
use types::{DaemonStatus, DownloadKind, DownloadStatus, DownloadTask, PartInfo, TorrentInfo};

struct HttpActive {
    cancel_token: CancellationToken,
    parts: Arc<Mutex<Vec<PartInfo>>>,
    #[allow(dead_code)]
    downloaded_atomic: Arc<AtomicU64>,
}

pub struct YoutubeActive {
    pub cancel_token: CancellationToken,
    pub downloaded_atomic: Arc<AtomicU64>,
}

pub struct Engine {
    config: Arc<RwLock<Config>>,
    http: Arc<HttpDownloader>,
    torrent: Arc<TorrentManager>,
    youtube: Arc<YoutubeManager>,
    tasks: Arc<RwLock<HashMap<String, DownloadTask>>>,
    http_active: Arc<Mutex<HashMap<String, HttpActive>>>,
    torrent_active: Arc<Mutex<HashMap<String, Arc<librqbit::ManagedTorrent>>>>,
    youtube_active: Arc<Mutex<HashMap<String, YoutubeActive>>>,
    last_speeds: Arc<Mutex<HashMap<String, (u64, u64, Instant)>>>,
}

impl Engine {
    pub async fn new(config: Arc<RwLock<Config>>) -> Result<Arc<Self>> {
        let download_dir = {
            let cfg = config.read().await;
            cfg.download_dir.clone()
        };

        let torrent = Arc::new(TorrentManager::new(download_dir).await?);
        let http = Arc::new(HttpDownloader::new());
        let youtube = Arc::new(YoutubeManager::new());
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let http_active = Arc::new(Mutex::new(HashMap::new()));
        let torrent_active = Arc::new(Mutex::new(HashMap::new()));
        let youtube_active = Arc::new(Mutex::new(HashMap::new()));
        let last_speeds = Arc::new(Mutex::new(HashMap::new()));

        let engine = Arc::new(Self {
            config,
            http,
            torrent,
            youtube,
            tasks,
            http_active,
            torrent_active,
            youtube_active,
            last_speeds,
        });

        // Load tasks from storage
        engine.load_saved_tasks().await;

        // Spawn background monitoring loop (every 1 sec)
        let engine_clone = Arc::clone(&engine);
        tokio::spawn(async move {
            engine_clone.monitor_loop().await;
        });

        Ok(engine)
    }

    fn storage_path() -> PathBuf {
        let base = dirs::data_dir().unwrap_or_else(|| {
            dirs::home_dir()
                .map(|h| h.join(".local/share"))
                .unwrap_or_else(|| PathBuf::from("."))
        });
        base.join("panda-dl").join("tasks.json")
    }

    async fn load_saved_tasks(&self) {
        let path = Self::storage_path();
        if !path.exists() {
            return;
        }

        if let Ok(data) = std::fs::read_to_string(&path) {
            if let Ok(mut saved) = serde_json::from_str::<HashMap<String, DownloadTask>>(&data) {
                for (_, task) in saved.iter_mut() {
                    // Reset transient status
                    if task.status == DownloadStatus::Downloading {
                        task.status = DownloadStatus::Paused;
                    }
                    task.download_speed = 0;
                    task.upload_speed = 0;
                }
                let mut map = self.tasks.write().await;
                *map = saved;
            }
        }
    }

    pub async fn save_tasks(&self) -> Result<()> {
        let path = Self::storage_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let map = self.tasks.read().await;
        let data = serde_json::to_string_pretty(&*map)?;
        std::fs::write(&path, data)?;
        Ok(())
    }

    pub async fn add_download(
        self: &Arc<Self>,
        uri: &str,
        custom_dir: Option<PathBuf>,
        parts_count: Option<usize>,
    ) -> Result<String> {
        let uri = uri.trim();
        let is_youtube = uri.contains("youtube.com/watch") || uri.contains("youtu.be/");
        let is_torrent = !is_youtube && (uri.starts_with("magnet:?")
            || uri.ends_with(".torrent")
            || Path::new(uri).extension().map(|e| e == "torrent").unwrap_or(false));

        let default_dir = {
            let cfg = self.config.read().await;
            cfg.download_dir.clone()
        };
        let target_dir = custom_dir.unwrap_or(default_dir);
        std::fs::create_dir_all(&target_dir)?;

        let id = uuid::Uuid::new_v4().to_string();

        if is_youtube {
            let id_clone = id.clone();
            let uri_clone = uri.to_string();
            let target_dir_clone = target_dir.clone();
            let engine = self.clone();

            tokio::spawn(async move {
                let mut initial_name = "YouTube Video".to_string();
                let downloaded_atomic = Arc::new(AtomicU64::new(0));
                let cancel_token = CancellationToken::new();

                let task = DownloadTask {
                    id: id_clone.clone(),
                    name: initial_name.clone(),
                    uri: uri_clone.clone(),
                    kind: DownloadKind::Youtube,
                    status: DownloadStatus::Queued,
                    total_bytes: None,
                    downloaded_bytes: 0,
                    uploaded_bytes: 0,
                    download_speed: 0,
                    upload_speed: 0,
                    eta_seconds: None,
                    output_path: target_dir_clone.to_string_lossy().into_owned(),
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    parts: vec![],
                    torrent_info: None,
                };

                engine.tasks.write().await.insert(id_clone.clone(), task);
                let _ = engine.save_tasks().await;
                
                if let Some(title) = crate::engine::youtube::YoutubeManager::get_title(&uri_clone).await {
                    initial_name = title.clone();
                    if let Some(t) = engine.tasks.write().await.get_mut(&id_clone) {
                        t.name = title;
                    }
                }

                if let Some(t) = engine.tasks.write().await.get_mut(&id_clone) {
                    t.status = DownloadStatus::Downloading;
                }
                
                let active = YoutubeActive {
                    cancel_token: cancel_token.clone(),
                    downloaded_atomic: downloaded_atomic.clone(),
                };
                engine.youtube_active.lock().await.insert(id_clone.clone(), active);

                let res = engine.youtube.download(&uri_clone, &target_dir_clone, downloaded_atomic.clone(), cancel_token.clone()).await;
                
                engine.youtube_active.lock().await.remove(&id_clone);
                
                let mut tasks = engine.tasks.write().await;
                if let Some(t) = tasks.get_mut(&id_clone) {
                    if cancel_token.is_cancelled() {
                        t.status = DownloadStatus::Paused;
                    } else if let Err(ref e) = res {
                        t.status = DownloadStatus::Error(e.to_string());
                    } else {
                        t.status = DownloadStatus::Completed;
                        t.downloaded_bytes = downloaded_atomic.load(std::sync::atomic::Ordering::Relaxed);
                    }
                }
                drop(tasks);
                let _ = engine.save_tasks().await;
                
                if res.is_ok() && !cancel_token.is_cancelled() {
                    send_notification("YouTube Download Complete", &initial_name);
                }
            });
            return Ok(id);
        } else if is_torrent {
            let initial_name = if uri.starts_with("magnet:?") {
                uri.split("&dn=")
                    .nth(1)
                    .and_then(|s| s.split('&').next())
                    .map(|s| urlencoding::decode(s).unwrap_or_else(|_| s.into()).into_owned())
                    .unwrap_or_else(|| "Magnet Download".to_string())
            } else {
                Path::new(uri)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Torrent Download")
                    .to_string()
            };

            let task = DownloadTask {
                id: id.clone(),
                name: initial_name,
                uri: uri.to_string(),
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
            let id_clone = id.clone();
            let uri_clone = uri.to_string();
            let target_dir_clone = target_dir.clone();

            tokio::spawn(async move {
                match engine.torrent.add(&uri_clone, Some(&target_dir_clone)).await {
                    Ok(handle) => {
                        let hash_hex = handle
                            .info_hash()
                            .0
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();

                        let mut tasks = engine.tasks.write().await;
                        if let Some(t) = tasks.get_mut(&id_clone) {
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

                        engine.torrent_active.lock().await.insert(id_clone, handle);
                        let _ = engine.save_tasks().await;
                        info!("Torrent added and active: {}", uri_clone);
                    }
                    Err(e) => {
                        error!("Failed to resolve/add torrent: {}", e);
                        let mut tasks = engine.tasks.write().await;
                        if let Some(t) = tasks.get_mut(&id_clone) {
                            t.status = DownloadStatus::Error(e.to_string());
                        }
                        drop(tasks);
                        let _ = engine.save_tasks().await;
                    }
                }
            });

            return Ok(id);
        }

        // HTTP/HTTPS Download
        let probe = self.http.probe(uri).await?;
        let output_path = HttpDownloader::resolve_unique_path(&target_dir, &probe.filename);

        let default_parts = {
            let cfg = self.config.read().await;
            cfg.default_parts
        };
        let num_parts = parts_count.unwrap_or(default_parts);

        let parts = if probe.supports_ranges && probe.total_bytes.map(|b| b > 1024 * 1024).unwrap_or(false) {
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

        // Create pre-allocated file if length is known and range supported
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

        self.start_http_download(&id, uri, &output_path, parts, probe.supports_ranges).await;

        Ok(id)
    }

    async fn start_http_download(
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

        // Keep a second Arc reference so we can read final part values
        // after the download task finishes (parts_shared is moved into the spawned task).
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

            // Read the final part state from our retained Arc reference
            let final_parts: Vec<PartInfo> = parts_reader.lock().await.clone();
            engine.http_active.lock().await.remove(&id_owned);

            let mut tasks = engine.tasks.write().await;
            if let Some(task) = tasks.get_mut(&id_owned) {
                if cancel_token.is_cancelled() {
                    // Preserve progress so resume can continue from here
                    task.parts = final_parts;
                    task.status = DownloadStatus::Paused;
                } else if let Err(e) = res {
                    task.parts = final_parts;
                    error!("Download failed for {}: {}", task.name, e);
                    task.status = DownloadStatus::Error(e.to_string());
                } else {
                    // Mark all chunks fully complete → chunk bar goes fully green
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
                    send_notification("Download Completed", &format!("{} finished downloading!", task.name));
                }
            }
            drop(tasks);
            let _ = engine.save_tasks().await;
        });
    }

    pub async fn pause_download(&self, id: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let task = match tasks.get_mut(id) {
            Some(t) => t,
            None => bail!("Task not found"),
        };

        match task.kind {
            DownloadKind::Http => {
                if let Some(active) = self.http_active.lock().await.remove(id) {
                    active.cancel_token.cancel();
                }
                task.status = DownloadStatus::Paused;
                task.download_speed = 0;
            }
            DownloadKind::Youtube => {
                if let Some(active) = self.youtube_active.lock().await.remove(id) {
                    active.cancel_token.cancel();
                }
                task.status = DownloadStatus::Paused;
                task.download_speed = 0;
            }
            DownloadKind::Torrent => {
                if let Some(handle) = self.torrent_active.lock().await.get(id) {
                    self.torrent.pause(handle).await?;
                }
                task.status = DownloadStatus::Paused;
                task.download_speed = 0;
                task.upload_speed = 0;
            }
        }

        drop(tasks);
        self.save_tasks().await?;
        Ok(())
    }

    pub async fn resume_download(self: &Arc<Self>, id: &str) -> Result<()> {
        let (kind, uri, output_path, parts) = {
            let mut tasks = self.tasks.write().await;
            let task = match tasks.get_mut(id) {
                Some(t) => t,
                None => bail!("Task not found"),
            };

            if task.status != DownloadStatus::Paused && !matches!(task.status, DownloadStatus::Error(_)) {
                return Ok(());
            }

            task.status = DownloadStatus::Downloading;
            (task.kind.clone(), task.uri.clone(), PathBuf::from(&task.output_path), task.parts.clone())
        };

        match kind {
            DownloadKind::Http => {
                let supports_ranges = parts.len() > 1;
                self.start_http_download(id, &uri, &output_path, parts, supports_ranges).await;
            }
            DownloadKind::Youtube => {
                let downloaded_atomic = Arc::new(AtomicU64::new(0));
                let cancel_token = CancellationToken::new();

                let active = YoutubeActive {
                    cancel_token: cancel_token.clone(),
                    downloaded_atomic: downloaded_atomic.clone(),
                };
                self.youtube_active.lock().await.insert(id.to_string(), active);

                let id_clone = id.to_string();
                let uri_clone = uri.clone();
                let engine = self.clone();
                let target_dir = output_path.clone();
                
                tokio::spawn(async move {
                    let res = engine.youtube.download(&uri_clone, &target_dir, downloaded_atomic.clone(), cancel_token.clone()).await;
                    engine.youtube_active.lock().await.remove(&id_clone);
                    let mut tasks = engine.tasks.write().await;
                    if let Some(t) = tasks.get_mut(&id_clone) {
                        if cancel_token.is_cancelled() {
                            t.status = DownloadStatus::Paused;
                        } else if let Err(ref e) = res {
                            t.status = DownloadStatus::Error(e.to_string());
                        } else {
                            t.status = DownloadStatus::Completed;
                            t.downloaded_bytes = downloaded_atomic.load(std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    let _ = engine.save_tasks().await;
                });
            }
            DownloadKind::Torrent => {
                let active = self.torrent_active.lock().await;
                if let Some(handle) = active.get(id) {
                    self.torrent.unpause(handle).await?;
                } else {
                    drop(active);
                    let handle = self.torrent.add(&uri, Some(&output_path)).await?;
                    self.torrent_active.lock().await.insert(id.to_string(), handle);
                }
            }
        }

        self.save_tasks().await?;
        Ok(())
    }

    pub async fn remove_download(&self, id: &str, delete_file: bool) -> Result<()> {
        // Stop if active
        if let Some(active) = self.http_active.lock().await.remove(id) {
            active.cancel_token.cancel();
        }
        
        if let Some(active) = self.youtube_active.lock().await.remove(id) {
            active.cancel_token.cancel();
        }

        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.remove(id) {
            if let Some(handle) = self.torrent_active.lock().await.remove(id) {
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

        drop(tasks);
        self.save_tasks().await?;
        Ok(())
    }

    pub async fn get_status(&self) -> DaemonStatus {
        let tasks = self.tasks.read().await;
        let mut list: Vec<DownloadTask> = tasks.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        let active_count = list.iter().filter(|t| t.status == DownloadStatus::Downloading).count();
        let total_down: u64 = list.iter().map(|t| t.download_speed).sum();
        let total_up: u64 = list.iter().map(|t| t.upload_speed).sum();

        DaemonStatus {
            active_downloads: active_count,
            total_download_speed: total_down,
            total_upload_speed: total_up,
            downloads: list,
        }
    }

    async fn monitor_loop(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            interval.tick().await;

            let mut tasks = self.tasks.write().await;
            let mut speeds = self.last_speeds.lock().await;

            // Update HTTP tasks
            let http_active = self.http_active.lock().await;
            for (id, active) in http_active.iter() {
                if let Some(task) = tasks.get_mut(id) {
                    let parts = active.parts.lock().await;
                    task.parts = parts.clone();
                    let current_downloaded: u64 = parts.iter().map(|p| p.downloaded).sum();
                    drop(parts);

                    task.downloaded_bytes = current_downloaded;

                    let now = Instant::now();
                    let (prev_down, prev_up, prev_time) = speeds
                        .get(id)
                        .copied()
                        .unwrap_or((0, 0, now - std::time::Duration::from_secs(1)));

                    let elapsed = now.duration_since(prev_time).as_secs_f64().max(0.1);
                    let delta_bytes = current_downloaded.saturating_sub(prev_down);
                    let speed = (delta_bytes as f64 / elapsed) as u64;

                    task.download_speed = speed;
                    if let Some(total) = task.total_bytes {
                        if speed > 0 && total > current_downloaded {
                            task.eta_seconds = Some((total - current_downloaded) / speed);
                        } else {
                            task.eta_seconds = None;
                        }
                    }

                    speeds.insert(id.clone(), (current_downloaded, prev_up, now));
                }
            }
            drop(http_active);

            // Update YouTube tasks
            let youtube_active = self.youtube_active.lock().await;
            for (id, active) in youtube_active.iter() {
                if let Some(task) = tasks.get_mut(id) {
                    let current_downloaded = active.downloaded_atomic.load(std::sync::atomic::Ordering::Relaxed);
                    task.downloaded_bytes = current_downloaded;

                    let now = Instant::now();
                    let (prev_down, prev_up, prev_time) = speeds
                        .get(id)
                        .copied()
                        .unwrap_or((0, 0, now - std::time::Duration::from_secs(1)));

                    let elapsed = now.duration_since(prev_time).as_secs_f64().max(0.1);
                    let delta_bytes = current_downloaded.saturating_sub(prev_down);
                    let speed = (delta_bytes as f64 / elapsed) as u64;

                    task.download_speed = speed;
                    if let Some(total) = task.total_bytes {
                        if speed > 0 && total > current_downloaded {
                            task.eta_seconds = Some((total - current_downloaded) / speed);
                        } else {
                            task.eta_seconds = None;
                        }
                    }

                    speeds.insert(id.clone(), (current_downloaded, prev_up, now));
                }
            }
            drop(youtube_active);

            // Update Torrent tasks
            let torrent_active = self.torrent_active.lock().await;
            for (id, handle) in torrent_active.iter() {
                if let Some(task) = tasks.get_mut(id) {
                    let stats = handle.stats();
                    task.total_bytes = Some(stats.total_bytes);
                    task.downloaded_bytes = stats.progress_bytes;
                    task.uploaded_bytes = stats.uploaded_bytes;

                    if let Some(name) = handle.name() {
                        if task.name.starts_with("Torrent_") || task.name == "Magnet Download" {
                            task.name = name;
                        }
                    }

                    if stats.finished && task.status == DownloadStatus::Downloading {
                        task.status = DownloadStatus::Completed;
                        send_notification("Torrent Download Complete", &format!("{} finished downloading!", task.name));
                    }

                    let now = Instant::now();
                    let (prev_down, prev_up, prev_time) = speeds
                        .get(id)
                        .copied()
                        .unwrap_or((0, 0, now - std::time::Duration::from_secs(1)));

                    let elapsed = now.duration_since(prev_time).as_secs_f64().max(0.1);
                    let delta_down = stats.progress_bytes.saturating_sub(prev_down);
                    let delta_up = stats.uploaded_bytes.saturating_sub(prev_up);

                    task.download_speed = (delta_down as f64 / elapsed) as u64;
                    task.upload_speed = (delta_up as f64 / elapsed) as u64;

                    if task.download_speed > 0 && stats.total_bytes > stats.progress_bytes {
                        task.eta_seconds = Some((stats.total_bytes - stats.progress_bytes) / task.download_speed);
                    } else {
                        task.eta_seconds = None;
                    }

                    speeds.insert(id.clone(), (stats.progress_bytes, stats.uploaded_bytes, now));
                }
            }
            drop(torrent_active);
        }
    }
}

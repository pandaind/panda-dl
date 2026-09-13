use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{info, warn};


pub struct ClipboardWatcher {
    last_captured: Arc<Mutex<(String, Instant)>>,
    enabled: Arc<AtomicBool>,
}

impl ClipboardWatcher {
    pub fn new(enabled: bool) -> Self {
        Self {
            last_captured: Arc::new(Mutex::new((String::new(), Instant::now() - Duration::from_secs(100)))),
            enabled: Arc::new(AtomicBool::new(enabled)),
        }
    }

    #[allow(dead_code)]
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn is_downloadable(text: &str) -> Option<DetectedKind> {
        let trimmed = text.trim();
        if trimmed.starts_with("magnet:?") && trimmed.contains("xt=urn:btih:") {
            return Some(DetectedKind::Magnet);
        }

        if let Ok(parsed) = url::Url::parse(trimmed) {
            let scheme = parsed.scheme();
            if scheme == "http" || scheme == "https" || scheme == "ftp" {
                let path = parsed.path().to_lowercase();
                if path.ends_with(".torrent") {
                    return Some(DetectedKind::TorrentUrl);
                }

                let extensions = [
                    ".iso", ".zip", ".tar.gz", ".tar.xz", ".tgz", ".7z", ".rar",
                    ".bin", ".appimage", ".dmg", ".pkg", ".deb", ".rpm",
                    ".exe", ".msi", ".mkv", ".mp4", ".mov", ".flac", ".mp3",
                ];
                for ext in &extensions {
                    if path.ends_with(ext) {
                        return Some(DetectedKind::DirectFile);
                    }
                }
            }
        }

        None
    }

    pub async fn start_listener<F, Fut>(self: Arc<Self>, on_detected: F)
    where
        F: Fn(String, DetectedKind) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            info!("Starting Wayland clipboard watcher (wl-paste --watch)...");

            loop {
                // Run wl-paste in watch mode
                let mut child = match Command::new("wl-paste")
                    .arg("--watch")
                    .arg("cat")
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to spawn wl-paste: {}. Retrying in 5 seconds...", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                if let Some(stdout) = child.stdout.take() {
                    let mut reader = BufReader::new(stdout).lines();

                    while let Ok(Some(line)) = reader.next_line().await {
                        if !self.is_enabled() {
                            continue;
                        }

                        let text = line.trim().to_string();
                        if let Some(kind) = Self::is_downloadable(&text) {
                            let mut last = self.last_captured.lock().await;
                            let (prev_text, prev_time) = &*last;

                            // Deduplicate: don't re-trigger if same URL was captured in last 30s
                            if prev_text == &text && prev_time.elapsed() < Duration::from_secs(30) {
                                continue;
                            }

                            *last = (text.clone(), Instant::now());
                            drop(last);

                            info!("Auto-captured {:?}: {}", kind, text);
                            on_detected(text, kind).await;
                        }
                    }
                }

                let _ = child.wait().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedKind {
    Magnet,
    TorrentUrl,
    DirectFile,
}

pub fn send_notification(title: &str, body: &str) {
    let title_owned = title.to_string();
    let body_owned = body.to_string();

    tokio::spawn(async move {
        let _ = Command::new("notify-send")
            .arg("-a")
            .arg("Panda Downloader")
            .arg("-i")
            .arg("download")
            .arg(&title_owned)
            .arg(&body_owned)
            .status()
            .await;
    });
}

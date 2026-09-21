use anyhow::{anyhow, Result};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use regex::Regex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct YoutubeManager {}

impl YoutubeManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn parse_size(size_str: &str, unit: &str) -> u64 {
        let val: f64 = size_str.trim().replace('~', "").parse().unwrap_or(0.0);
        match unit.trim() {
            "B" => val as u64,
            "KiB" => (val * 1024.0) as u64,
            "MiB" => (val * 1024.0 * 1024.0) as u64,
            "GiB" => (val * 1024.0 * 1024.0 * 1024.0) as u64,
            _ => val as u64,
        }
    }

    pub async fn download(
        &self,
        uri: &str,
        output_dir: &std::path::Path,
        downloaded_atomic: Arc<AtomicU64>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let out_template = output_dir.join("%(title)s.%(ext)s");
        let mut child = Command::new("yt-dlp")
            .arg("--newline")
            .arg("-o")
            .arg(out_template.to_string_lossy().as_ref())
            .arg(uri)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| anyhow!("Failed to spawn yt-dlp: {}", e))?;

        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout).lines();

        let re = Regex::new(r"\[download\]\s+([\d\.]+)%\s+of\s+[~]?\s*([\d\.]+)(KiB|MiB|GiB|B|TiB)").unwrap();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    let _ = child.kill().await;
                    return Ok(());
                }
                res = reader.next_line() => {
                    match res {
                        Ok(Some(line)) => {
                            if let Some(caps) = re.captures(&line) {
                                if let (Some(pct_str), Some(sz_str), Some(unit)) = (caps.get(1), caps.get(2), caps.get(3)) {
                                    let pct: f64 = pct_str.as_str().parse().unwrap_or(0.0);
                                    let total_sz = Self::parse_size(sz_str.as_str(), unit.as_str());
                                    let downloaded = (total_sz as f64 * (pct / 100.0)) as u64;
                                    downloaded_atomic.store(downloaded, Ordering::Relaxed);
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            warn!("yt-dlp read error: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        let status = child.wait().await?;
        if !status.success() {
            return Err(anyhow!("yt-dlp failed with status {}", status));
        }

        Ok(())
    }

    pub async fn get_title(uri: &str) -> Option<String> {
        let out = Command::new("yt-dlp")
            .arg("--get-title")
            .arg(uri)
            .output()
            .await
            .ok()?;
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
        None
    }
}

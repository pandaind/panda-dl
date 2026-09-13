use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, RANGE};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::types::PartInfo;

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub filename: String,
    pub total_bytes: Option<u64>,
    pub supports_ranges: bool,
}

pub struct HttpDownloader {
    client: reqwest::Client,
}

impl HttpDownloader {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) PandaDL/1.0")
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self { client }
    }

    pub async fn probe(&self, url: &str) -> Result<ProbeResult> {
        // Strategy 1: Range 0-0 request — tells us length + range support in one shot
        if let Ok(result) = self.probe_with_range(url).await {
            return Ok(result);
        }
        // Strategy 2: HEAD request
        if let Ok(result) = self.probe_with_head(url).await {
            return Ok(result);
        }
        // Strategy 3: Simple GET (no range support, unknown length)
        self.probe_with_get(url).await
    }

    async fn probe_with_range(&self, url: &str) -> Result<ProbeResult> {
        let resp = self
            .client
            .get(url)
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .context("Range probe failed")?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let filename = Self::extract_filename(url, &headers);

        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            let total = headers.get("content-range")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.split('/').last())
                .and_then(|v| v.trim().parse::<u64>().ok());
            return Ok(ProbeResult { filename, total_bytes: total, supports_ranges: true });
        }
        if status.is_success() {
            let cl = headers.get(CONTENT_LENGTH)
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            let ranges = headers.get(ACCEPT_RANGES)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false);
            return Ok(ProbeResult { filename, total_bytes: cl, supports_ranges: ranges });
        }
        bail!("Range probe HTTP {}", status)
    }

    async fn probe_with_head(&self, url: &str) -> Result<ProbeResult> {
        let resp = self
            .client
            .head(url)
            .send()
            .await
            .context("HEAD probe failed")?;

        if !resp.status().is_success() {
            bail!("HEAD probe HTTP {}", resp.status());
        }
        let headers = resp.headers().clone();
        let filename = Self::extract_filename(url, &headers);
        let cl = headers.get(CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        let ranges = headers.get(ACCEPT_RANGES)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);
        Ok(ProbeResult { filename, total_bytes: cl, supports_ranges: ranges })
    }

    async fn probe_with_get(&self, url: &str) -> Result<ProbeResult> {
        // GET without Range — least preferred; still gives us filename
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("GET probe failed")?;

        if !resp.status().is_success() {
            bail!("GET probe HTTP {}", resp.status());
        }
        let headers = resp.headers().clone();
        let filename = Self::extract_filename(url, &headers);
        let cl = headers.get(CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        Ok(ProbeResult { filename, total_bytes: cl, supports_ranges: false })
    }

    fn extract_filename(url_str: &str, headers: &HeaderMap) -> String {
        // 1. Content-Disposition
        if let Some(cd) = headers.get(CONTENT_DISPOSITION).and_then(|h| h.to_str().ok()) {
            if let Some(name) = Self::parse_content_disposition(cd) {
                if !name.trim().is_empty() {
                    return Self::sanitize_filename(&name);
                }
            }
        }

        // 2. URL path segment
        if let Ok(parsed) = url::Url::parse(url_str) {
            if let Some(segments) = parsed.path_segments() {
                if let Some(last) = segments.last() {
                    let decoded = urlencoding::decode(last).unwrap_or_else(|_| last.into());
                    let trimmed = decoded.trim();
                    if !trimmed.is_empty() && trimmed != "/" {
                        return Self::sanitize_filename(trimmed);
                    }
                }
            }
        }

        "download.bin".to_string()
    }

    fn parse_content_disposition(cd: &str) -> Option<String> {
        // filename*=UTF-8''...
        for part in cd.split(';') {
            let part = part.trim();
            if part.starts_with("filename*=") {
                let val = part.trim_start_matches("filename*=").trim_matches('"');
                if let Some(idx) = val.find("''") {
                    let encoded = &val[idx + 2..];
                    if let Ok(decoded) = urlencoding::decode(encoded) {
                        return Some(decoded.into_owned());
                    }
                }
            } else if part.starts_with("filename=") {
                let val = part.trim_start_matches("filename=").trim_matches('"');
                return Some(val.to_string());
            }
        }
        None
    }

    pub fn sanitize_filename(name: &str) -> String {
        let clean: String = name
            .chars()
            .map(|c| if c == '/' || c == '\\' || c == '\0' { '_' } else { c })
            .collect();
        let trimmed = clean.trim_matches(&['.', ' ', '-'][..]);
        if trimmed.is_empty() {
            "download.bin".to_string()
        } else {
            trimmed.to_string()
        }
    }

    pub fn resolve_unique_path(dir: &Path, filename: &str) -> PathBuf {
        let target = dir.join(filename);
        if !target.exists() {
            return target;
        }

        let stem = Path::new(filename)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("download");
        let ext = Path::new(filename)
            .extension()
            .and_then(|s| s.to_str())
            .map(|e| format!(".{}", e))
            .unwrap_or_default();

        for i in 1..10000 {
            let candidate = dir.join(format!("{} ({}){}", stem, i, ext));
            if !candidate.exists() {
                return candidate;
            }
        }

        dir.join(format!("{}_{}", uuid::Uuid::new_v4(), filename))
    }

    pub fn create_parts(total_bytes: u64, num_parts: usize) -> Vec<PartInfo> {
        let count = num_parts.clamp(1, 64);
        let part_size = total_bytes / count as u64;
        let mut parts = Vec::with_capacity(count);

        for i in 0..count {
            let start = i as u64 * part_size;
            let end = if i == count - 1 {
                total_bytes - 1
            } else {
                (i as u64 + 1) * part_size - 1
            };

            parts.push(PartInfo {
                index: i,
                start,
                end,
                downloaded: 0,
                active: false,
            });
        }

        parts
    }

    pub async fn download_parts(
        &self,
        url: &str,
        output_path: &Path,
        parts: Arc<Mutex<Vec<PartInfo>>>,
        downloaded_bytes_atomic: Arc<AtomicU64>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(output_path)
            .context("Failed to open output file for writing parts")?;
        let shared_file = Arc::new(file);

        let parts_count = {
            let p = parts.lock().await;
            p.len()
        };

        let mut tasks = Vec::new();

        for index in 0..parts_count {
            let client = self.client.clone();
            let url = url.to_string();
            let shared_file = Arc::clone(&shared_file);
            let parts = Arc::clone(&parts);
            let downloaded_atomic = Arc::clone(&downloaded_bytes_atomic);
            let cancel = cancel_token.clone();

            tasks.push(tokio::spawn(async move {
                let (start, end, already_downloaded) = {
                    let mut p = parts.lock().await;
                    if let Some(part) = p.get_mut(index) {
                        part.active = true;
                        (part.start, part.end, part.downloaded)
                    } else {
                        return Ok::<(), anyhow::Error>(());
                    }
                };

                let mut current_offset = start + already_downloaded;
                if current_offset > end {
                    // Part already complete
                    let mut p = parts.lock().await;
                    if let Some(part) = p.get_mut(index) {
                        part.active = false;
                    }
                    return Ok(());
                }

                let mut retries = 0;
                let max_retries = 5;

                loop {
                    if cancel.is_cancelled() {
                        let mut p = parts.lock().await;
                        if let Some(part) = p.get_mut(index) {
                            part.active = false;
                        }
                        return Ok(());
                    }

                    let req = client
                        .get(&url)
                        .header(RANGE, format!("bytes={}-{}", current_offset, end))
                        .send();

                    tokio::select! {
                        _ = cancel.cancelled() => {
                            let mut p = parts.lock().await;
                            if let Some(part) = p.get_mut(index) {
                                part.active = false;
                            }
                            return Ok(());
                        }
                        res = req => {
                            match res {
                                Ok(response) if response.status().is_success() => {
                                    let mut stream = response.bytes_stream();
                                    let mut part_success = true;

                                    while let Some(chunk_res) = stream.next().await {
                                        if cancel.is_cancelled() {
                                            part_success = false;
                                            break;
                                        }

                                        match chunk_res {
                                            Ok(chunk) => {
                                                let chunk_len = chunk.len() as u64;
                                                let write_pos = current_offset;

                                                let file_for_write = Arc::clone(&shared_file);
                                                let write_res = tokio::task::spawn_blocking(move || {
                                                    file_for_write.write_all_at(&chunk, write_pos)
                                                }).await;

                                                if let Ok(Ok(())) = write_res {
                                                    current_offset += chunk_len;
                                                    downloaded_atomic.fetch_add(chunk_len, Ordering::Relaxed);

                                                    let mut p = parts.lock().await;
                                                    if let Some(part) = p.get_mut(index) {
                                                        part.downloaded += chunk_len;
                                                    }
                                                } else {
                                                    error!("Failed to write chunk at offset {}", write_pos);
                                                    part_success = false;
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Stream error on part {}: {}", index, e);
                                                part_success = false;
                                                break;
                                            }
                                        }
                                    }

                                    if part_success && current_offset > end {
                                        let mut p = parts.lock().await;
                                        if let Some(part) = p.get_mut(index) {
                                            // Ensure downloaded reflects exact part size (guards against
                                            // off-by-one from atomic vs mutex accounting)
                                            part.downloaded = part.end - part.start + 1;
                                            part.active = false;
                                        }
                                        return Ok(());
                                    }
                                }
                                Ok(response) => {
                                    warn!("Server returned {} on part {}", response.status(), index);
                                }
                                Err(e) => {
                                    warn!("Request error on part {}: {}", index, e);
                                }
                            }
                        }
                    }

                    retries += 1;
                    if retries > max_retries {
                        let mut p = parts.lock().await;
                        if let Some(part) = p.get_mut(index) {
                            part.active = false;
                        }
                        bail!("Max retries exceeded for part {}", index);
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(500 * (1 << retries))).await;
                }
            }));
        }

        for task in tasks {
            if let Err(e) = task.await? {
                if !cancel_token.is_cancelled() {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub async fn download_single(
        &self,
        url: &str,
        output_path: &Path,
        downloaded_bytes_atomic: Arc<AtomicU64>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("Failed to send request for single-stream download")?;

        if !resp.status().is_success() {
            bail!("Server returned HTTP {}", resp.status());
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path)
            .context("Failed to create single stream output file")?;
        let shared_file = Arc::new(file);

        let mut stream = resp.bytes_stream();
        let mut current_offset = 0u64;

        while let Some(chunk_res) = stream.next().await {
            if cancel_token.is_cancelled() {
                return Ok(());
            }

            let chunk = chunk_res?;
            let chunk_len = chunk.len() as u64;
            let write_pos = current_offset;

            let file_for_write = Arc::clone(&shared_file);
            tokio::task::spawn_blocking(move || {
                file_for_write.write_all_at(&chunk, write_pos)
            })
            .await
            .context("Task join error")?
            .context("File write error")?;

            current_offset += chunk_len;
            downloaded_bytes_atomic.fetch_add(chunk_len, Ordering::Relaxed);
        }

        Ok(())
    }
}

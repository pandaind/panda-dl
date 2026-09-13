use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadKind {
    Http,
    Torrent,
    Youtube,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartInfo {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentInfo {
    pub info_hash: String,
    pub seeds: usize,
    pub peers: usize,
    pub total_pieces: usize,
    pub finished_pieces: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: String,
    pub name: String,
    pub uri: String,
    pub kind: DownloadKind,
    pub status: DownloadStatus,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub eta_seconds: Option<u64>,
    pub output_path: String,
    pub created_at: u64,
    pub parts: Vec<PartInfo>,
    pub torrent_info: Option<TorrentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub active_downloads: usize,
    pub total_download_speed: u64,
    pub total_upload_speed: u64,
    pub downloads: Vec<DownloadTask>,
}

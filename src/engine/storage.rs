use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;

use super::types::{DownloadStatus, DownloadTask};

/// Returns the path to the persisted tasks JSON file.
pub fn storage_path() -> PathBuf {
    let base = dirs::data_dir().unwrap_or_else(|| {
        dirs::home_dir()
            .map(|h| h.join(".local/share"))
            .unwrap_or_else(|| PathBuf::from("."))
    });
    base.join("panda-dl").join("tasks.json")
}

/// Load previously saved tasks from disk.
///
/// Downloads that were `Downloading` are reset to `Paused` because the daemon
/// has just (re)started and those transfers are no longer active.
pub fn load_tasks() -> HashMap<String, DownloadTask> {
    let path = storage_path();
    if !path.exists() {
        return HashMap::new();
    }

    if let Ok(data) = std::fs::read_to_string(&path) {
        if let Ok(mut saved) = serde_json::from_str::<HashMap<String, DownloadTask>>(&data) {
            for task in saved.values_mut() {
                if task.status == DownloadStatus::Downloading {
                    task.status = DownloadStatus::Paused;
                }
                task.download_speed = 0;
                task.upload_speed = 0;
            }
            return saved;
        }
    }

    HashMap::new()
}

/// Persist the current task map to disk.
pub fn save_tasks(tasks: &HashMap<String, DownloadTask>) -> Result<()> {
    let path = storage_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(tasks)?;
    std::fs::write(&path, data)?;
    Ok(())
}

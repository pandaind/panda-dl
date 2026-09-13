use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub download_dir: PathBuf,
    pub default_parts: usize,
    pub max_concurrent_downloads: usize,
    pub auto_capture_clipboard: bool,
    pub auto_start_magnets: bool,
    pub auto_start_torrents: bool,
    pub show_desktop_notifications: bool,
}

impl Default for Config {
    fn default() -> Self {
        let download_dir = dirs::download_dir().unwrap_or_else(|| {
            dirs::home_dir()
                .map(|h| h.join("Downloads"))
                .unwrap_or_else(|| PathBuf::from("/tmp"))
        });

        Self {
            download_dir,
            default_parts: 16,
            max_concurrent_downloads: 5,
            auto_capture_clipboard: true,
            auto_start_magnets: true,
            auto_start_torrents: true,
            show_desktop_notifications: true,
        }
    }
}

impl Config {
    pub fn config_path() -> PathBuf {
        let base = dirs::config_dir().unwrap_or_else(|| {
            dirs::home_dir()
                .map(|h| h.join(".config"))
                .unwrap_or_else(|| PathBuf::from("."))
        });
        base.join("panda-dl").join("config.json")
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Ok(cfg) = serde_json::from_str::<Config>(&content) {
                    return cfg;
                }
            }
        }
        let default = Self::default();
        let _ = default.save();
        default
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, data)?;
        Ok(())
    }
}

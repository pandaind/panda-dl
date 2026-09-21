use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "panda-dl", author = "pandac", version, about = "Fast multi-part & torrent downloader for Omarchy")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the background daemon
    Daemon,
    /// Ensure daemon is running in background
    Start,
    /// Add a download (URL, Magnet link, or .torrent file)
    Add {
        /// URI to download
        uri: String,
        /// Optional download directory
        #[arg(short, long)]
        dir: Option<PathBuf>,
        /// Number of parallel connection parts (default: 16)
        #[arg(short, long)]
        parts: Option<usize>,
    },
    /// List all downloads
    List,
    /// Print JSON status (used by Omarchy bar widget)
    Status,
    /// Pause a download
    Pause {
        id: String,
    },
    /// Resume a download
    Resume {
        id: String,
    },
    /// Remove a download
    Remove {
        id: String,
        #[arg(long)]
        delete_file: bool,
    },
    /// Open downloaded file in default application
    OpenFile {
        id: String,
    },
    /// Open download containing folder in file manager
    OpenFolder {
        id: String,
    },
    /// Get current configuration
    GetConfig,
    /// Set configuration options
    SetConfig {
        #[arg(long)]
        dir: Option<PathBuf>,
        #[arg(long)]
        parts: Option<usize>,
        #[arg(long)]
        auto_capture: Option<bool>,
        #[arg(long)]
        auto_magnets: Option<bool>,
        #[arg(long)]
        auto_torrents: Option<bool>,
    },
    /// Install desktop integration & register magnet/torrent handlers
    InstallDesktop,
}

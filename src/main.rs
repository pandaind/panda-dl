mod capture;
mod config;
mod daemon;
mod engine;
mod ipc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;


use ipc::{IpcClient, Request, Response};

#[derive(Parser)]
#[command(name = "panda-dl", author = "pandac", version, about = "Fast multi-part & torrent downloader for Omarchy")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Daemon) => {
            daemon::run_daemon().await?;
        }
        Some(Commands::Start) => {
            ensure_daemon_running().await?;
            println!("Panda-DL daemon is running.");
        }
        Some(Commands::Add { uri, dir, parts }) => {
            ensure_daemon_running().await?;
            let req = Request::Add {
                uri,
                dir: dir.map(|d| d.display().to_string()),
                parts,
            };
            match IpcClient::send(req).await? {
                Response::Ok { id, message } => {
                    println!("{} (ID: {})", message.unwrap_or_default(), id.unwrap_or_default());
                }
                Response::Error { message } => {
                    eprintln!("Error: {}", message);
                }
                _ => {}
            }
        }
        Some(Commands::List) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::Status).await? {
                Response::Status(status) => {
                    println!("============================================================");
                    println!(
                        "Panda-DL | Active: {} | Down: {:.2} MB/s | Up: {:.2} MB/s",
                        status.active_downloads,
                        status.total_download_speed as f64 / 1_048_576.0,
                        status.total_upload_speed as f64 / 1_048_576.0,
                    );
                    println!("============================================================");
                    if status.downloads.is_empty() {
                        println!("No downloads found.");
                    } else {
                        for t in status.downloads {
                            let total_str = t
                                .total_bytes
                                .map(|b| format!("{:.2} MB", b as f64 / 1_048_576.0))
                                .unwrap_or_else(|| "Unknown".to_string());
                            let pct = t
                                .total_bytes
                                .map(|tot| {
                                    if tot > 0 {
                                        format!("{:.1}%", (t.downloaded_bytes as f64 / tot as f64) * 100.0)
                                    } else {
                                        "0%".to_string()
                                    }
                                })
                                .unwrap_or_else(|| "---".to_string());

                            let speed_str = if t.download_speed > 0 {
                                format!("{:.2} MB/s", t.download_speed as f64 / 1_048_576.0)
                            } else {
                                "-".to_string()
                            };

                            println!(
                                "[{}] {:<12} | {:<25} | {}/{} ({}) | {}",
                                &t.id[..8],
                                format!("{:?}", t.status),
                                if t.name.len() > 25 { &t.name[..25] } else { &t.name },
                                format!("{:.2} MB", t.downloaded_bytes as f64 / 1_048_576.0),
                                total_str,
                                pct,
                                speed_str,
                            );
                        }
                    }
                }
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::Status) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::Status).await? {
                Response::Status(status) => {
                    println!("{}", serde_json::to_string(&status)?);
                }
                Response::Error { message } => {
                    println!("{}", serde_json::json!({ "error": message }));
                }
                _ => {}
            }
        }
        Some(Commands::Pause { id }) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::Pause { id }).await? {
                Response::Ok { message, .. } => println!("{}", message.unwrap_or_default()),
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::Resume { id }) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::Resume { id }).await? {
                Response::Ok { message, .. } => println!("{}", message.unwrap_or_default()),
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::Remove { id, delete_file }) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::Remove { id, delete_file }).await? {
                Response::Ok { message, .. } => println!("{}", message.unwrap_or_default()),
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::OpenFile { id }) => {
            ensure_daemon_running().await?;
            let _ = IpcClient::send(Request::OpenFile { id }).await?;
        }
        Some(Commands::OpenFolder { id }) => {
            ensure_daemon_running().await?;
            let _ = IpcClient::send(Request::OpenFolder { id }).await?;
        }
        Some(Commands::GetConfig) => {
            ensure_daemon_running().await?;
            match IpcClient::send(Request::GetConfig).await? {
                Response::Config(cfg) => println!("{}", serde_json::to_string_pretty(&cfg)?),
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::SetConfig {
            dir,
            parts,
            auto_capture,
            auto_magnets,
            auto_torrents,
        }) => {
            ensure_daemon_running().await?;
            let mut cfg = match IpcClient::send(Request::GetConfig).await? {
                Response::Config(c) => c,
                _ => config::Config::load(),
            };

            if let Some(d) = dir {
                cfg.download_dir = d;
            }
            if let Some(p) = parts {
                cfg.default_parts = p;
            }
            if let Some(ac) = auto_capture {
                cfg.auto_capture_clipboard = ac;
            }
            if let Some(am) = auto_magnets {
                cfg.auto_start_magnets = am;
            }
            if let Some(at) = auto_torrents {
                cfg.auto_start_torrents = at;
            }

            match IpcClient::send(Request::SetConfig { config: cfg }).await? {
                Response::Config(updated) => {
                    println!("Updated configuration:\n{}", serde_json::to_string_pretty(&updated)?);
                }
                Response::Error { message } => eprintln!("Error: {}", message),
                _ => {}
            }
        }
        Some(Commands::InstallDesktop) => {
            install_desktop_integration()?;
        }
        None => {
            println!("Panda-DL v0.1.0 — High speed multi-part & torrent downloader");
            println!("Run 'panda-dl --help' for available commands.");
        }
    }

    Ok(())
}

async fn ensure_daemon_running() -> Result<()> {
    let socket_path = ipc::IpcServer::socket_path();
    if socket_path.exists() {
        if tokio::net::UnixStream::connect(&socket_path).await.is_ok() {
            return Ok(());
        }
        let _ = std::fs::remove_file(&socket_path);
    }

    let exe = std::env::current_exe().context("Failed to get current executable path")?;
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }

    cmd.spawn().context("Failed to spawn daemon process")?;

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if socket_path.exists() {
            if tokio::net::UnixStream::connect(&socket_path).await.is_ok() {
                return Ok(());
            }
        }
    }

    anyhow::bail!("Timed out waiting for Panda-DL daemon to start");
}

fn install_desktop_integration() -> Result<()> {
    let exe = std::env::current_exe()?;
    let exe_path = exe.display().to_string();

    let apps_dir = dirs::data_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap().join(".local/share"))
        .join("applications");
    std::fs::create_dir_all(&apps_dir)?;

    let desktop_content = format!(
        r#"[Desktop Entry]
Name=Panda Downloader
Comment=Multi-part and BitTorrent download manager
Exec={exe_path} add %u
Terminal=false
Type=Application
Icon=download
MimeType=x-scheme-handler/magnet;application/x-bittorrent;
Categories=Network;FileTransfer;
"#
    );

    let desktop_file = apps_dir.join("panda-dl.desktop");
    std::fs::write(&desktop_file, desktop_content)?;
    println!("Created {}", desktop_file.display());

    let _ = std::process::Command::new("xdg-mime")
        .args(["default", "panda-dl.desktop", "x-scheme-handler/magnet"])
        .status();

    let _ = std::process::Command::new("xdg-mime")
        .args(["default", "panda-dl.desktop", "application/x-bittorrent"])
        .status();

    println!("Registered MIME handlers for magnet: and .torrent files.");
    Ok(())
}

mod capture;
mod cli;
mod config;
mod daemon;
mod engine;
mod ipc;

use anyhow::Result;
use clap::Parser;

use cli::{
    commands::{Cli, Commands},
    handlers,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Daemon) => {
            daemon::run_daemon().await?;
        }
        Some(Commands::Start) => {
            handlers::handle_start().await?;
        }
        Some(Commands::Add { uri, dir, parts }) => {
            handlers::handle_add(uri, dir, parts).await?;
        }
        Some(Commands::List) => {
            handlers::handle_list().await?;
        }
        Some(Commands::Status) => {
            handlers::handle_status().await?;
        }
        Some(Commands::Pause { id }) => {
            handlers::handle_pause(id).await?;
        }
        Some(Commands::Resume { id }) => {
            handlers::handle_resume(id).await?;
        }
        Some(Commands::Remove { id, delete_file }) => {
            handlers::handle_remove(id, delete_file).await?;
        }
        Some(Commands::OpenFile { id }) => {
            handlers::handle_open_file(id).await?;
        }
        Some(Commands::OpenFolder { id }) => {
            handlers::handle_open_folder(id).await?;
        }
        Some(Commands::GetConfig) => {
            handlers::handle_get_config().await?;
        }
        Some(Commands::SetConfig {
            dir,
            parts,
            auto_capture,
            auto_magnets,
            auto_torrents,
        }) => {
            handlers::handle_set_config(dir, parts, auto_capture, auto_magnets, auto_torrents)
                .await?;
        }
        Some(Commands::InstallDesktop) => {
            handlers::handle_install_desktop()?;
        }
        None => {
            println!(
                "Panda-DL v{} — High speed multi-part & torrent downloader",
                env!("CARGO_PKG_VERSION")
            );
            println!("Run 'panda-dl --help' for available commands.");
        }
    }

    Ok(())
}

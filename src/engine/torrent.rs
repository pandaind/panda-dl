use anyhow::{bail, Context, Result};
use librqbit::{
    AddTorrent, AddTorrentOptions, ManagedTorrent, Session, SessionOptions,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct TorrentManager {
    session: Arc<Session>,
    output_dir: PathBuf,
}

impl TorrentManager {
    pub async fn new(output_dir: PathBuf) -> Result<Self> {
        let opts = SessionOptions::default();
        let session = Session::new_with_opts(output_dir.clone(), opts)
            .await
            .context("Failed to initialize librqbit session")?;

        Ok(Self {
            session,
            output_dir,
        })
    }

    #[allow(dead_code)]
    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    pub async fn add(&self, uri: &str, custom_dir: Option<&Path>) -> Result<Arc<ManagedTorrent>> {
        let folder = custom_dir.map(|p| p.to_path_buf()).unwrap_or_else(|| self.output_dir.clone());
        let opts = AddTorrentOptions {
            output_folder: Some(folder.display().to_string()),
            overwrite: true,
            ..Default::default()
        };

        let add = AddTorrent::from_cli_argument(uri)?;
        let resp = self.session.add_torrent(add, Some(opts)).await
            .context("Failed to add torrent to librqbit session")?;

        match resp.into_handle() {
            Some(handle) => Ok(handle),
            None => bail!("Could not get handle for torrent"),
        }
    }

    pub async fn pause(&self, handle: &Arc<ManagedTorrent>) -> Result<()> {
        self.session.pause(handle).await
            .context("Failed to pause torrent")?;
        Ok(())
    }

    pub async fn unpause(&self, handle: &Arc<ManagedTorrent>) -> Result<()> {
        self.session.unpause(handle).await
            .context("Failed to unpause torrent")?;
        Ok(())
    }

    pub async fn remove(&self, handle: &ManagedTorrent, delete_files: bool) -> Result<()> {
        let id = handle.id();
        self.session.delete(librqbit::api::TorrentIdOrHash::Id(id), delete_files).await
            .context("Failed to delete torrent")?;
        Ok(())
    }
}

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::server::IpcServer;
use super::types::{Request, Response};

/// Thin client that sends a single request to the daemon over the Unix socket
/// and returns the response.
pub struct IpcClient;

impl IpcClient {
    pub async fn send(req: Request) -> Result<Response> {
        let path = IpcServer::socket_path();
        if !path.exists() {
            bail!(
                "Panda-DL daemon is not running (socket not found at {})",
                path.display()
            );
        }

        let stream = UnixStream::connect(&path)
            .await
            .context("Failed to connect to Panda-DL socket")?;

        let (reader, mut writer) = stream.into_split();
        let payload = serde_json::to_string(&req)? + "\n";
        writer.write_all(payload.as_bytes()).await?;

        let mut lines = BufReader::new(reader).lines();
        if let Some(line) = lines.next_line().await? {
            let resp: Response = serde_json::from_str(&line)?;
            Ok(resp)
        } else {
            bail!("Connection closed without response");
        }
    }
}

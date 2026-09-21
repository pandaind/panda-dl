use tokio::process::Command;

/// Send a desktop notification via `notify-send`.
pub fn send_notification(title: &str, body: &str) {
    let title_owned = title.to_string();
    let body_owned = body.to_string();

    tokio::spawn(async move {
        let _ = Command::new("notify-send")
            .arg("-a")
            .arg("Panda Downloader")
            .arg("-i")
            .arg("download")
            .arg(&title_owned)
            .arg(&body_owned)
            .status()
            .await;
    });
}

# Panda-DL 🐼

A high-performance, daemonized download manager for Linux, written in Rust.

Panda-DL handles HTTP downloads, BitTorrent, and YouTube video extraction via a background daemon. It communicates via a fast Unix Socket IPC, allowing lightweight frontends (like a CLI or desktop widgets) to instantly add, pause, and query downloads without keeping a terminal open.

## Features

- **Multi-protocol Support**: 
  - **HTTP/HTTPS**: Multi-connection chunked downloads for maximum speed.
  - **BitTorrent**: Native torrent and magnet link support powered by `librqbit`.
  - **YouTube/Media**: Video and audio extraction powered by `yt-dlp`.
- **Daemon Architecture**: Downloads run completely in the background. Close your terminal, log out, or put your laptop to sleep—the daemon manages it all.
- **Fast IPC**: Frontend clients communicate with the daemon via lightning-fast Unix sockets.
- **Auto-Clipboard Watcher**: Automatically detects links copied to your clipboard (Wayland `wl-paste` integration) and adds them to your download queue.
- **Desktop Integration**: Can register as the default application for Magnet links and `.torrent` files in your Linux desktop environment.

## Installation

Ensure you have Rust and Cargo installed, as well as `yt-dlp` and `wl-clipboard` (for clipboard watching).

```bash
cargo build --release
cp target/release/panda-dl ~/.local/bin/
```

### Desktop Integration
To make Panda-DL the default handler for Magnet links and `.torrent` files on your Linux desktop:
```bash
panda-dl install-desktop
```

## Usage

Start the background daemon. (Note: Many frontends, like the Omarchy widget, will auto-start this for you).
```bash
panda-dl daemon
```

Alternatively, detach it completely:
```bash
panda-dl start
```

### CLI Commands

Add a new download (HTTP, Magnet, or YouTube):
```bash
panda-dl add "https://example.com/file.iso"
panda-dl add "magnet:?xt=urn:btih:..."
panda-dl add "https://youtube.com/watch?v=..."
```

Control active downloads:
```bash
panda-dl pause <ID>
panda-dl resume <ID>
panda-dl remove <ID>          # Removes the task from the list
panda-dl remove <ID> --clean  # Removes the task AND deletes the downloaded file
```

Query status (returns JSON containing speeds, ETAs, and progress for all active downloads):
```bash
panda-dl status
```

Open the destination folder of a download in your default file manager:
```bash
panda-dl open-folder <ID>
```

## Desktop Widgets

Panda-DL is designed to be used with desktop widgets! If you use the Omarchy shell, you can install the official plugin for a beautiful, interactive GUI:

```bash
omarchy plugin add https://github.com/pandaind/omarchy-panda-dl
```

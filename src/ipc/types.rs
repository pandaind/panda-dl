use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::engine::types::DaemonStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Request {
    Status,
    Add {
        uri: String,
        #[serde(default)]
        dir: Option<String>,
        #[serde(default)]
        parts: Option<usize>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Remove {
        id: String,
        #[serde(default)]
        delete_file: bool,
    },
    OpenFile {
        id: String,
    },
    OpenFolder {
        id: String,
    },
    GetConfig,
    SetConfig {
        config: Config,
    },
    ClipboardEvent {
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Status(DaemonStatus),
    Config(Config),
    Error {
        message: String,
    },
}

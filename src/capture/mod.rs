pub mod clipboard;
pub mod notification;

pub use clipboard::ClipboardWatcher;
#[allow(unused_imports)]
pub use clipboard::DetectedKind;
pub use notification::send_notification;

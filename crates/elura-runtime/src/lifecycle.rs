//! Process lifecycle helpers shared by generated Gateway and World binaries.

use std::io;

/// Waits for the platform's normal process termination signal.
///
/// Unix processes accept both `SIGINT` and `SIGTERM`; other platforms use the
/// Tokio Ctrl-C facility. The caller remains responsible for notifying its
/// watch-based server tasks so each runtime can drain within its configured
/// deadline.
#[cfg(unix)]
pub async fn shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
pub async fn shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

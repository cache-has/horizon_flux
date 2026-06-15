// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::path::PathBuf;

use tokio::signal;
use tracing::info;

/// Wait for a shutdown signal (SIGINT or SIGTERM on Unix, Ctrl+C
/// everywhere).
///
/// Returns when the first signal is received.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("Received SIGINT"),
        () = terminate => info!("Received SIGTERM"),
    }
}

/// RAII guard that removes the lockfile when dropped.
///
/// Ensures cleanup on normal shutdown, panics, and signal-triggered
/// cancellation. The only case NOT covered is SIGKILL, which is handled
/// by stale-lockfile detection on the next startup.
pub struct LockfileGuard {
    path: PathBuf,
}

impl LockfileGuard {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for LockfileGuard {
    fn drop(&mut self) {
        crate::lockfile::remove(&self.path);
        info!("Lockfile removed: {}", self.path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockfile_guard_removes_file_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance.lock");
        let info = crate::lockfile::InstanceInfo { pid: 1, port: 8080 };
        crate::lockfile::write(&path, &info).unwrap();
        assert!(path.exists());

        {
            let _guard = LockfileGuard::new(path.clone());
        } // guard drops here

        assert!(!path.exists());
    }
}

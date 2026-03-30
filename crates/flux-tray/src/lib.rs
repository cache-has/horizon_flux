// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! System tray icon and desktop notifications for Horizon Flux.
//!
//! Runs the native event loop on a dedicated OS thread and communicates
//! with the async server via channels. Degrades gracefully when tray APIs
//! are unavailable (e.g. headless servers).

mod icon;
mod menu;
mod notification;
pub mod prefs;

use std::thread;

use crossbeam_channel::{Receiver, Sender};
use flux_datafusion::{ExecutionEvent, RunStatus};
use tracing::{info, warn};

/// Commands sent from the async world to the tray thread.
#[derive(Debug)]
pub enum TrayCommand {
    /// Update the server URL (used for "Open" menu item).
    SetUrl(String),
    /// Shut down the tray event loop.
    Shutdown,
}

/// Handle returned to the caller for communicating with the tray thread.
pub struct TrayHandle {
    cmd_tx: Sender<TrayCommand>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TrayHandle {
    /// Tell the tray to open a specific URL when "Open Horizon Flux" is clicked.
    pub fn set_url(&self, url: &str) {
        let _ = self.cmd_tx.send(TrayCommand::SetUrl(url.to_string()));
    }

    /// Get a clone of the command sender for use in callbacks.
    pub fn cmd_sender(&self) -> Sender<TrayCommand> {
        self.cmd_tx.clone()
    }

    /// Shut down the tray thread and wait for it to exit.
    pub fn shutdown(mut self) {
        let _ = self.cmd_tx.send(TrayCommand::Shutdown);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TrayHandle {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(TrayCommand::Shutdown);
        // Best-effort join on drop; don't block indefinitely.
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Configuration for the tray.
pub struct TrayConfig {
    /// The version string to display in the menu.
    pub version: String,
}

/// Spawn the system tray on a dedicated thread.
///
/// Subscribes to `event_rx` for pipeline execution events and listens
/// for commands on the returned [`TrayHandle`].
///
/// Returns `None` if the tray could not be initialized (headless, missing
/// libraries, etc.) — the application should continue without it.
pub fn spawn(
    config: TrayConfig,
    event_rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
) -> Option<TrayHandle> {
    let (cmd_tx, cmd_rx) = crossbeam_channel::bounded::<TrayCommand>(32);

    let handle = thread::Builder::new()
        .name("horizon-tray".into())
        .spawn(move || {
            // On macOS, muda::Menu requires the main thread. Catch panics
            // so we degrade gracefully instead of crashing.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_tray_loop(config, event_rx, cmd_rx)
            }));
            match result {
                Ok(Err(e)) => warn!("Tray thread exited with error: {e}"),
                Err(_) => warn!("Tray thread panicked (likely macOS main-thread requirement) — continuing without tray"),
                Ok(Ok(())) => {}
            }
        });

    match handle {
        Ok(join_handle) => {
            info!("System tray started");
            Some(TrayHandle {
                cmd_tx,
                thread: Some(join_handle),
            })
        }
        Err(e) => {
            warn!("Failed to spawn tray thread: {e}");
            None
        }
    }
}

/// Main loop running on the tray thread.
fn run_tray_loop(
    config: TrayConfig,
    mut event_rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
    cmd_rx: Receiver<TrayCommand>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut prefs = prefs::TrayPrefs::load();
    let tray_menu = menu::build_menu(&config.version, prefs.notifications_enabled);
    let icon_data = icon::idle_icon();

    let _tray = match tray_icon::TrayIconBuilder::new()
        .with_tooltip("Horizon Flux")
        .with_icon(icon_data)
        .with_menu(Box::new(tray_menu.menu.clone()))
        .build()
    {
        Ok(t) => t,
        Err(e) => {
            warn!("Could not create system tray icon: {e}");
            return Ok(());
        }
    };

    let menu_event_rx = tray_icon::menu::MenuEvent::receiver();
    let mut url = String::new();
    let mut recent_runs: Vec<RecentRun> = Vec::new();

    info!("Tray event loop running");

    loop {
        // 1. Check for menu clicks.
        if let Ok(event) = menu_event_rx.try_recv() {
            if event.id() == tray_menu.open_item.id() {
                if !url.is_empty() {
                    let _ = open::that(&url);
                }
            } else if event.id() == tray_menu.notifications_item.id() {
                // CheckMenuItem toggles its own checked state on click.
                prefs.notifications_enabled = tray_menu.notifications_item.is_checked();
                prefs.save();
                info!(
                    "Notifications {}",
                    if prefs.notifications_enabled { "enabled" } else { "disabled" }
                );
            } else if event.id() == tray_menu.stop_item.id() {
                info!("Stop Server requested from tray menu");
                // Send SIGINT to ourselves for graceful shutdown.
                #[cfg(unix)]
                unsafe {
                    libc::raise(libc::SIGINT);
                }
                #[cfg(not(unix))]
                {
                    // On Windows, generate a ctrl-c event.
                    // The server's shutdown_signal() handler will catch it.
                    unsafe {
                        #[cfg(target_os = "windows")]
                        windows_sys::Win32::System::Console::GenerateConsoleCtrlEvent(0, 0);
                    }
                }
                break;
            } else {
                // Check if it's a recent-run item — open that run's URL.
                for run in &recent_runs {
                    if *event.id() == run.menu_id {
                        if !url.is_empty() {
                            let run_url = format!("{}/pipelines?run={}", url, run.run_id);
                            let _ = open::that(&run_url);
                        }
                        break;
                    }
                }
            }
        }

        // 2. Check for pipeline execution events.
        match event_rx.try_recv() {
            Ok(event) => {
                handle_execution_event(
                    &event,
                    &mut recent_runs,
                    &tray_menu,
                    &_tray,
                    prefs.notifications_enabled,
                );
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                warn!("Tray missed {n} execution events");
            }
            Err(_) => {}
        }

        // 3. Check for commands from the async world.
        if let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                TrayCommand::SetUrl(u) => {
                    url = u;
                }
                TrayCommand::Shutdown => {
                    info!("Tray received shutdown command");
                    break;
                }
            }
        }

        // Sleep briefly to avoid busy-spinning.
        thread::sleep(std::time::Duration::from_millis(50));
    }

    info!("Tray event loop exiting");
    Ok(())
}

/// A recently completed pipeline run shown in the tray submenu.
struct RecentRun {
    run_id: String,
    menu_id: tray_icon::menu::MenuId,
    #[allow(dead_code)]
    label: String,
}

/// Process a pipeline execution event: update tray icon, fire notifications,
/// and maintain the recent-runs list.
fn handle_execution_event(
    event: &ExecutionEvent,
    recent_runs: &mut Vec<RecentRun>,
    tray_menu: &menu::TrayMenu,
    tray: &tray_icon::TrayIcon,
    notifications_enabled: bool,
) {
    match event {
        ExecutionEvent::RunStarted { pipeline_name, .. } => {
            let _ = tray.set_icon(Some(icon::running_icon()));
            let tooltip = format!("Horizon Flux — running {pipeline_name}");
            let _ = tray.set_tooltip(Some(&tooltip));
        }
        ExecutionEvent::RunCompleted {
            run_id,
            status,
            duration_ms,
        } => {
            match status {
                RunStatus::Success => {
                    let _ = tray.set_icon(Some(icon::idle_icon()));
                    let _ = tray.set_tooltip(Some("Horizon Flux"));
                    if notifications_enabled {
                        notification::send_success(run_id, *duration_ms);
                    }
                }
                RunStatus::Failed => {
                    let _ = tray.set_icon(Some(icon::error_icon()));
                    let _ = tray.set_tooltip(Some("Horizon Flux — last run failed"));
                    if notifications_enabled {
                        notification::send_failure(run_id, None);
                    }
                }
                _ => {
                    let _ = tray.set_icon(Some(icon::idle_icon()));
                    let _ = tray.set_tooltip(Some("Horizon Flux"));
                }
            }

            // Update recent runs (keep last 5).
            let status_str = status.as_str();
            let label = format!("{} — {status_str}", run_id);
            let menu_id = tray_icon::menu::MenuId::new(format!("run-{run_id}"));
            recent_runs.insert(
                0,
                RecentRun {
                    run_id: run_id.to_string(),
                    menu_id: menu_id.clone(),
                    label: label.clone(),
                },
            );
            recent_runs.truncate(5);

            // Rebuild the recent-runs submenu.
            menu::update_recent_runs(&tray_menu.recent_submenu, recent_runs);
        }
        ExecutionEvent::NodeFailed {
            run_id,
            node_id,
            error,
        } => {
            let _ = tray.set_icon(Some(icon::error_icon()));
            let tooltip = format!("Horizon Flux — node {node_id} failed");
            let _ = tray.set_tooltip(Some(&tooltip));
            if notifications_enabled {
                notification::send_node_failure(run_id, node_id, error);
            }
        }
        _ => {}
    }
}

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

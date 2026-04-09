// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subprocess spawner and child-process [`Transport`] implementation.
//!
//! Spawns a plugin executable as described by its [`Manifest`], wires
//! stdin/stdout into framed I/O, drains stderr into `tracing`, and forwards
//! `Log` frames into `tracing` so they don't clog the protocol channel.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{debug, error, info, trace, warn};

use crate::discovery::DiscoveredPlugin;
use crate::error::{Error, Result};
use crate::manifest::Manifest;
use crate::protocol::control::{Log, LogLevel};
use crate::protocol::{Frame, MessageKind, read_frame, write_frame};
use crate::transport::{Transport, TransportError};

/// Tunable knobs for [`PluginProcess::spawn`]. v1 ships defaults that match
/// the protocol doc; callers may override per-call as needed.
#[derive(Debug, Clone, Default)]
pub struct SpawnOptions {
    /// Extra args appended *after* the manifest's `args`.
    pub extra_args: Vec<String>,
    /// Working directory override. If `None`, the plugin directory is used.
    pub cwd: Option<PathBuf>,
}

/// A live plugin subprocess. Implements [`Transport`] so it can be handed to
/// [`crate::session::PluginSession`].
pub struct PluginProcess {
    name: String,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    rx: Receiver<ReaderEvent>,
    reader: Option<JoinHandle<()>>,
    stderr: Option<JoinHandle<()>>,
}

enum ReaderEvent {
    Frame(Frame),
    Closed,
}

impl PluginProcess {
    /// Spawn the plugin's executable. Validates the discovered plugin first
    /// and returns a process that is ready for [`crate::session::PluginSession`].
    pub fn spawn(plugin: &DiscoveredPlugin, opts: SpawnOptions) -> Result<Self> {
        let manifest = plugin.manifest.as_ref().ok_or_else(|| Error::Manifest {
            path: plugin.directory.clone(),
            message: "plugin has no valid manifest; cannot spawn".into(),
        })?;
        Self::spawn_with_manifest(&plugin.name, &plugin.directory, manifest, opts)
    }

    /// Lower-level spawn that takes the manifest pieces directly. Used by
    /// `spawn` and by tests that build a manifest in-memory.
    pub fn spawn_with_manifest(
        name: &str,
        plugin_dir: &std::path::Path,
        manifest: &Manifest,
        opts: SpawnOptions,
    ) -> Result<Self> {
        let exe = manifest.resolve_executable(plugin_dir);
        if !exe.is_file() {
            return Err(Error::Io {
                path: exe.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "plugin executable not found",
                ),
            });
        }

        let cwd = opts.cwd.unwrap_or_else(|| plugin_dir.to_path_buf());
        let mut cmd = Command::new(&exe);
        cmd.args(&manifest.args)
            .args(&opts.extra_args)
            .envs(manifest.env.iter())
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|source| Error::Io {
            path: exe.clone(),
            source,
        })?;

        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take().expect("stderr piped");

        let (tx, rx) = channel::<ReaderEvent>();
        let plugin_name = name.to_string();
        let reader = thread::Builder::new()
            .name(format!("plugin-reader[{plugin_name}]"))
            .spawn(move || reader_loop(stdout, tx, plugin_name))
            .map_err(|source| Error::Io {
                path: exe.clone(),
                source,
            })?;

        let plugin_name = name.to_string();
        let stderr_thread = thread::Builder::new()
            .name(format!("plugin-stderr[{plugin_name}]"))
            .spawn(move || stderr_loop(stderr, plugin_name))
            .map_err(|source| Error::Io {
                path: exe.clone(),
                source,
            })?;

        info!(plugin = %name, exe = %exe.display(), "spawned plugin");
        Ok(Self {
            name: name.to_string(),
            child: Some(child),
            stdin: Some(stdin),
            rx,
            reader: Some(reader),
            stderr: Some(stderr_thread),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

fn reader_loop(stdout: std::process::ChildStdout, tx: Sender<ReaderEvent>, plugin_name: String) {
    let mut r = BufReader::new(stdout);
    loop {
        match read_frame(&mut r) {
            Ok(frame) => {
                if frame.kind == MessageKind::Log {
                    match serde_json::from_slice::<Log>(&frame.payload) {
                        Ok(log) => forward_log(&plugin_name, log),
                        Err(e) => warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "plugin sent invalid Log payload"
                        ),
                    }
                    continue;
                }
                if tx.send(ReaderEvent::Frame(frame)).is_err() {
                    break;
                }
            }
            Err(crate::protocol::FrameError::UnexpectedEof { read: 0, .. })
            | Err(crate::protocol::FrameError::Io(_)) => {
                let _ = tx.send(ReaderEvent::Closed);
                break;
            }
            Err(e) => {
                error!(plugin = %plugin_name, error = %e, "plugin protocol error");
                let _ = tx.send(ReaderEvent::Closed);
                break;
            }
        }
    }
    debug!(plugin = %plugin_name, "reader loop exiting");
}

fn forward_log(plugin: &str, log: Log) {
    match log.level {
        LogLevel::Trace => trace!(target: "flux::plugin", plugin = %plugin, "{}", log.message),
        LogLevel::Debug => debug!(target: "flux::plugin", plugin = %plugin, "{}", log.message),
        LogLevel::Info => info!(target: "flux::plugin", plugin = %plugin, "{}", log.message),
        LogLevel::Warn => warn!(target: "flux::plugin", plugin = %plugin, "{}", log.message),
        LogLevel::Error => error!(target: "flux::plugin", plugin = %plugin, "{}", log.message),
    }
}

fn stderr_loop(stderr: std::process::ChildStderr, plugin_name: String) {
    let r = BufReader::new(stderr);
    for line in r.lines().map_while(std::result::Result::ok) {
        if !line.is_empty() {
            warn!(target: "flux::plugin::stderr", plugin = %plugin_name, "{}", line);
        }
    }
}

impl Transport for PluginProcess {
    fn send(
        &mut self,
        kind: MessageKind,
        payload: &[u8],
    ) -> std::result::Result<(), TransportError> {
        let stdin = self.stdin.as_mut().ok_or(TransportError::Closed)?;
        write_frame(stdin, kind, payload)?;
        stdin.flush()?;
        Ok(())
    }

    fn recv(
        &mut self,
        timeout: Duration,
        phase: &'static str,
    ) -> std::result::Result<Frame, TransportError> {
        match self.rx.recv_timeout(timeout) {
            Ok(ReaderEvent::Frame(frame)) => Ok(frame),
            Ok(ReaderEvent::Closed) => Err(TransportError::Closed),
            Err(RecvTimeoutError::Timeout) => Err(TransportError::Timeout { phase, timeout }),
            Err(RecvTimeoutError::Disconnected) => Err(TransportError::Closed),
        }
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        // Closing stdin lets a well-behaved plugin exit. We then wait briefly
        // and kill if it overstays.
        drop(self.stdin.take());
        if let Some(mut child) = self.child.take() {
            let killed = match child.try_wait() {
                Ok(Some(_)) => false,
                _ => {
                    let _ = child.kill();
                    true
                }
            };
            let _ = child.wait();
            if killed {
                debug!(plugin = %self.name, "killed plugin on drop");
            }
        }
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
        if let Some(h) = self.stderr.take() {
            let _ = h.join();
        }
    }
}

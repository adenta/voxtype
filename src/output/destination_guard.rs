//! Recording-scoped Hyprland destination tracking. No titles or field contents
//! are read. A blocked recording can never become eligible for insertion again.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const QUERY_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_REPLY: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockReason {
    FocusChanged,
    AuthenticationPrompt,
    DetectionFailed,
}

impl BlockReason {
    pub fn message(self) -> &'static str {
        match self {
            Self::FocusChanged => "Focus changed—dictation copied instead of pasted.",
            Self::AuthenticationPrompt => {
                "Authentication prompt detected—dictation copied instead of pasted."
            }
            Self::DetectionFailed => {
                "Destination detection failed—dictation copied instead of pasted."
            }
        }
    }
}

#[derive(Default)]
struct State {
    window: Option<String>,
    reason: Option<BlockReason>,
}

impl State {
    fn block(&mut self, reason: BlockReason) {
        // Prefer the more explanatory authentication reason if both events fire.
        if self.reason.is_none() || reason == BlockReason::AuthenticationPrompt {
            self.reason = Some(reason);
        }
    }

    fn event(&mut self, line: &str) {
        if line == "openlayer>>omarchy-polkit" {
            self.block(BlockReason::AuthenticationPrompt);
        } else if let Some(address) = line.strip_prefix("activewindowv2>>") {
            if self.window.as_deref() != Some(normalize_address(address)) {
                // During initialization there is no safe baseline yet: even a
                // brief switch before the snapshot must invalidate this recording.
                self.block(BlockReason::FocusChanged);
            }
        }
    }

    fn snapshot(&mut self, address: &str, authentication: bool) {
        if authentication {
            self.block(BlockReason::AuthenticationPrompt);
        }
        let address = normalize_address(address);
        if address.is_empty() || address == "0" {
            self.block(BlockReason::DetectionFailed);
        } else if let Some(original) = &self.window {
            if original != address {
                self.block(BlockReason::FocusChanged);
            }
        } else {
            self.window = Some(address.to_owned());
        }
    }
}

fn normalize_address(address: &str) -> &str {
    address.trim().trim_start_matches("0x")
}

pub struct DestinationGuard {
    state: Arc<Mutex<State>>,
    request_path: PathBuf,
    reader: Option<JoinHandle<()>>,
    fence: Option<mpsc::Sender<oneshot::Sender<()>>>,
}

impl Drop for DestinationGuard {
    fn drop(&mut self) {
        if let Some(reader) = &self.reader {
            reader.abort();
        }
    }
}

impl DestinationGuard {
    /// Used if output reaches the paste driver without a recording baseline.
    /// Missing context is never permission to synthesize keys.
    pub fn unavailable() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                reason: Some(BlockReason::DetectionFailed),
                ..State::default()
            })),
            request_path: PathBuf::new(),
            reader: None,
            fence: None,
        }
    }

    pub async fn start() -> Self {
        let (Some(runtime), Some(instance)) = (
            std::env::var_os("XDG_RUNTIME_DIR"),
            std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE"),
        ) else {
            return Self::unavailable();
        };
        Self::start_at(&PathBuf::from(runtime).join("hypr").join(instance)).await
    }

    async fn start_at(directory: &Path) -> Self {
        let stream = match tokio::time::timeout(
            QUERY_TIMEOUT,
            UnixStream::connect(directory.join(".socket2.sock")),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            _ => return Self::unavailable(),
        };
        let state = Arc::new(Mutex::new(State::default()));
        let reader_state = state.clone();
        let (fence, mut fences) = mpsc::channel::<oneshot::Sender<()>>(1);
        let reader = tokio::spawn(async move {
            let mut stream = BufReader::new(stream);
            let mut line = Vec::new();
            loop {
                // Keep partially read bytes when the barrier arm wins. Already
                // queued events take priority over acknowledging a final check.
                let mut limited = (&mut stream).take(64 * 1024 - line.len() as u64);
                tokio::select! {
                    biased;
                    read = limited.read_until(b'\n', &mut line) => {
                        match read {
                            Ok(n) if n > 0 && line.last() == Some(&b'\n') => {
                                if let Ok(event) = std::str::from_utf8(&line) {
                                    reader_state.lock().unwrap().event(event.trim_end());
                                    line.clear();
                                } else { break; }
                            }
                            _ => break,
                        }
                    }
                    Some(ack) = fences.recv() => {
                        if !line.is_empty() {
                            reader_state.lock().unwrap().block(BlockReason::DetectionFailed);
                        }
                        let _ = ack.send(());
                    }
                }
            }
            reader_state
                .lock()
                .unwrap()
                .block(BlockReason::DetectionFailed);
        });
        let guard = Self {
            state,
            request_path: directory.join(".socket.sock"),
            reader: Some(reader),
            fence: Some(fence),
        };
        // The event subscription is already live before either snapshot.
        guard.refresh().await;
        guard.drain_events().await;
        guard
    }

    async fn query(&self, command: &[u8]) -> Result<serde_json::Value, ()> {
        tokio::time::timeout(QUERY_TIMEOUT, async {
            let mut stream = UnixStream::connect(&self.request_path)
                .await
                .map_err(|_| ())?;
            stream.write_all(command).await.map_err(|_| ())?;
            let mut bytes = Vec::new();
            stream
                .take(MAX_REPLY + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| ())?;
            if bytes.len() as u64 > MAX_REPLY {
                return Err(());
            }
            serde_json::from_slice(&bytes).map_err(|_| ())
        })
        .await
        .map_err(|_| ())?
    }

    async fn refresh(&self) {
        let result = async {
            let window = self.query(b"j/activewindow").await?;
            let layers = self.query(b"j/layers").await?;
            let address = window.get("address").and_then(|v| v.as_str()).unwrap_or("");
            let monitors = layers.as_object().filter(|v| !v.is_empty()).ok_or(())?;
            let mut authentication = false;
            for monitor in monitors.values() {
                let levels = monitor
                    .get("levels")
                    .and_then(|v| v.as_object())
                    .ok_or(())?;
                for level in levels.values() {
                    for layer in level.as_array().ok_or(())? {
                        let namespace = layer.get("namespace").and_then(|v| v.as_str()).ok_or(())?;
                        authentication |= namespace == "omarchy-polkit";
                    }
                }
            }
            self.state.lock().unwrap().snapshot(address, authentication);
            // Catch a window switch during the layers query as well.
            let last = self.query(b"j/activewindow").await?;
            let address = last.get("address").and_then(|v| v.as_str()).ok_or(())?;
            self.state.lock().unwrap().snapshot(address, false);
            Ok::<_, ()>(())
        }
        .await;
        if result.is_err() {
            self.state
                .lock()
                .unwrap()
                .block(BlockReason::DetectionFailed);
        }
    }

    async fn drain_events(&self) {
        let Some(fence) = &self.fence else {
            return;
        };
        let (ack, received) = oneshot::channel();
        let result = tokio::time::timeout(QUERY_TIMEOUT, async {
            fence.send(ack).await.map_err(|_| ())?;
            received.await.map_err(|_| ())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            self.state
                .lock()
                .unwrap()
                .block(BlockReason::DetectionFailed);
        }
    }

    pub async fn check(&self) -> Option<BlockReason> {
        if self
            .reader
            .as_ref()
            .is_none_or(|reader| reader.is_finished())
        {
            self.state
                .lock()
                .unwrap()
                .block(BlockReason::DetectionFailed);
        }
        if self.state.lock().unwrap().reason.is_none() {
            self.refresh().await;
        }
        self.drain_events().await;
        self.state.lock().unwrap().reason
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_are_sticky_but_unrelated_events_are_ignored() {
        let mut state = State::default();
        state.snapshot("0xabc", false);
        for event in [
            "activewindowv2>>abc",
            "openlayer>>notifications",
            "openlayer>>voxtype-osd",
            "openlayer>>omarchy-bar",
        ] {
            state.event(event);
        }
        assert_eq!(state.reason, None);
        state.event("activewindowv2>>def");
        state.event("activewindowv2>>abc");
        state.snapshot("0xabc", false);
        assert_eq!(state.reason, Some(BlockReason::FocusChanged));
        state.event("openlayer>>omarchy-polkit");
        state.event("closelayer>>omarchy-polkit");
        assert_eq!(state.reason, Some(BlockReason::AuthenticationPrompt));
    }

    #[test]
    fn initialization_and_preexisting_prompt_are_conservative() {
        let mut state = State::default();
        state.event("activewindowv2>>abc");
        state.snapshot("0xabc", false);
        assert_eq!(state.reason, Some(BlockReason::FocusChanged));
        let mut next = State::default();
        next.snapshot("0xabc", true);
        assert_eq!(next.reason, Some(BlockReason::AuthenticationPrompt));
        let mut fresh = State::default();
        fresh.snapshot("0xabc", false);
        assert_eq!(fresh.reason, None);
    }

    #[tokio::test]
    async fn absent_monitor_fails_closed() {
        let guard = DestinationGuard::start_at(Path::new("/nonexistent/voxtype-test")).await;
        assert_eq!(guard.check().await, Some(BlockReason::DetectionFailed));
    }
}

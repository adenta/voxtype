//! Run tool/IPC integration checks in a child process with isolated environment.
//! No real keyboard, clipboard, notification service, microphone or API is used.
#![cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use voxtype::config::{Config, DeepgramConfig, OutputMode, TranscriptionEngine};
use voxtype::output::destination_guard::{BlockReason, DestinationGuard};
use voxtype::output::paste::PasteOutput;
use voxtype::output::{
    create_output_chain_with_guard, OutputDelivery, StreamingSession, TextOutput,
};

#[test]
fn destination_guard_integration() {
    let root = std::env::temp_dir().join(format!("vox-guard-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("bin")).unwrap();
    install_tools(&root);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "guarded_delivery_child",
            "--ignored",
            "--nocapture",
        ])
        .env("VOXTYPE_GUARD_TEST_ROOT", &root)
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", root.join("bin").display()),
        )
        .env("XDG_RUNTIME_DIR", &root)
        .env("HYPRLAND_INSTANCE_SIGNATURE", "test")
        .env("XDG_SESSION_TYPE", "wayland")
        .env("WAYLAND_DISPLAY", "test")
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn install_tools(root: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for (name, body) in [
        ("wl-copy", "test ! -e \"$VOXTYPE_GUARD_TEST_ROOT/fail-copy\" || exit 1\ncat > \"$VOXTYPE_GUARD_TEST_ROOT/clipboard\"\nprintf '%s\\n' \"$*\" >> \"$VOXTYPE_GUARD_TEST_ROOT/copies\"\n"),
        ("wl-paste", "case \"$*\" in *--list-types*) echo text/plain;; *) cat \"$VOXTYPE_GUARD_TEST_ROOT/clipboard\";; esac\n"),
        ("notify-send", "printf '%s\\n' \"$*\" >> \"$VOXTYPE_GUARD_TEST_ROOT/notices\"\necho 42\n"),
        ("wtype", "printf '%s\\n' \"$*\" >> \"$VOXTYPE_GUARD_TEST_ROOT/keys\"\n"),
        ("eitype", "printf '%s\\n' \"$*\" >> \"$VOXTYPE_GUARD_TEST_ROOT/keys\"\n"),
        ("ydotool", "printf '%s\\n' \"$*\" >> \"$VOXTYPE_GUARD_TEST_ROOT/keys\"\n"),
        ("dotool", "echo dotool >> \"$VOXTYPE_GUARD_TEST_ROOT/keys\"\n"),
    ] {
        let path = root.join("bin").join(name);
        std::fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

struct Desktop {
    snapshot: Arc<Mutex<(String, bool, bool)>>,
    event: Arc<tokio::sync::Mutex<Option<UnixStream>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Desktop {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Desktop {
    async fn new(root: &Path) -> Self {
        let dir = root.join("hypr/test");
        std::fs::create_dir_all(&dir).unwrap();
        let requests = UnixListener::bind(dir.join(".socket.sock")).unwrap();
        let events = UnixListener::bind(dir.join(".socket2.sock")).unwrap();
        let snapshot = Arc::new(Mutex::new(("0xabc".to_string(), false, false)));
        let current = snapshot.clone();
        let request_task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = requests.accept().await {
                let mut command = [0; 128];
                let n = socket.read(&mut command).await.unwrap();
                let (window, prompt, broken) = current.lock().unwrap().clone();
                let response = if broken {
                    "bad-json".to_string()
                } else if &command[..n] == b"j/activewindow" {
                    serde_json::json!({"address": window}).to_string()
                } else {
                    serde_json::json!({"DP-1":{"levels":{"3": if prompt { vec![serde_json::json!({"namespace":"omarchy-polkit"})] } else { vec![] }}}}).to_string()
                };
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let event = Arc::new(tokio::sync::Mutex::new(None));
        let event_out = event.clone();
        let event_task = tokio::spawn(async move {
            while let Ok((socket, _)) = events.accept().await {
                *event_out.lock().await = Some(socket);
            }
        });
        Self {
            snapshot,
            event,
            tasks: vec![request_task, event_task],
        }
    }
    async fn emit(&self, event: &str) {
        self.event
            .lock()
            .await
            .as_mut()
            .unwrap()
            .write_all(event.as_bytes())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    async fn guard(&self) -> Arc<DestinationGuard> {
        let guard = Arc::new(DestinationGuard::start().await);
        guard
    }
}
fn read(root: &Path, file: &str) -> String {
    std::fs::read_to_string(root.join(file)).unwrap_or_default()
}
fn reset(root: &Path) {
    for name in ["keys", "copies", "notices", "fail-copy"] {
        let _ = std::fs::remove_file(root.join(name));
    }
    std::fs::write(root.join("clipboard"), "previous clipboard").unwrap();
}
fn paste(guard: Arc<DestinationGuard>, delay: u32) -> PasteOutput {
    PasteOutput::new(
        true,
        None,
        Some("shift+insert".into()),
        0,
        delay,
        true,
        1500,
    )
    .with_destination_guard(Some(guard))
}
const NOTE: &str = "Guard test: café\n第二行";

#[test]
#[ignore = "spawned by destination_guard_integration with isolated tools"]
fn guarded_delivery_child() {
    let root = PathBuf::from(std::env::var_os("VOXTYPE_GUARD_TEST_ROOT").unwrap());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let desktop = Desktop::new(&root).await;
        // Normal delivery preserves paste, Enter, and clipboard restoration.
        reset(&root);
        let guard = desktop.guard().await;
        assert_eq!(guard.check().await, None);
        for event in [
            "openlayer>>notifications\n",
            "openlayer>>voxtype-osd\n",
            "activewindowv2>>abc\n",
        ] {
            desktop.emit(event).await;
        }
        assert_eq!(
            paste(guard, 1).output_delivery(NOTE).await.unwrap(),
            OutputDelivery::Inserted
        );
        assert_eq!(read(&root, "keys").lines().count(), 2);
        assert_eq!(read(&root, "clipboard"), "previous clipboard");
        assert!(read(&root, "notices").is_empty());

        // A transient switch invalidates the whole recording even after returning.
        reset(&root);
        let guard = desktop.guard().await;
        desktop
            .emit("activewindowv2>>def\nactivewindowv2>>abc\n")
            .await;
        assert_eq!(
            paste(guard, 1).output_delivery(NOTE).await.unwrap(),
            OutputDelivery::Clipboard
        );
        assert!(read(&root, "keys").is_empty());
        assert_eq!(read(&root, "copies").lines().count(), 1);
        assert!(read(&root, "notices").contains("Focus changed"));
        tokio::time::sleep(Duration::from_millis(1550)).await;
        assert_eq!(read(&root, "clipboard"), NOTE);

        // Preexisting prompt; cancellation must not rewind clipboard-only text.
        reset(&root);
        desktop.snapshot.lock().unwrap().1 = true;
        let guard = desktop.guard().await;
        assert_eq!(guard.check().await, Some(BlockReason::AuthenticationPrompt));
        let mut config = Config::default();
        config.output.destination_guard = true;
        config.output.mode = OutputMode::Paste;
        let chain = create_output_chain_with_guard(&config.output, None, Some(guard));
        let mut session = StreamingSession::new();
        session
            .commit_segment(&chain, NOTE, None, None, None)
            .await
            .unwrap();
        assert_eq!(session.typed_chars(), 0);
        assert_eq!(session.finalized_text(), NOTE);
        session.rewind().await.unwrap();
        assert!(read(&root, "keys").is_empty());
        assert!(read(&root, "notices").contains("Authentication prompt detected"));
        assert_eq!(read(&root, "notices").lines().count(), 1);
        desktop.snapshot.lock().unwrap().1 = false;
        drop(chain);

        // Prompt appearing during the clipboard-settling delay, then disappearing.
        reset(&root);
        let guard = desktop.guard().await;
        let task = tokio::spawn(async move { paste(guard, 200).output_delivery(NOTE).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while read(&root, "copies").is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        desktop
            .emit("openlayer>>omarchy-polkit\ncloselayer>>omarchy-polkit\n")
            .await;
        assert_eq!(task.await.unwrap().unwrap(), OutputDelivery::Clipboard);
        assert!(read(&root, "keys").is_empty());
        assert_eq!(read(&root, "clipboard"), NOTE);
        assert_eq!(read(&root, "notices").lines().count(), 1);

        // Snapshot catches a changed window even without an event.
        reset(&root);
        let guard = desktop.guard().await;
        desktop.snapshot.lock().unwrap().0 = "0xdef".into();
        assert_eq!(
            paste(guard, 1).output_delivery(NOTE).await.unwrap(),
            OutputDelivery::Clipboard
        );
        assert!(read(&root, "keys").is_empty());
        desktop.snapshot.lock().unwrap().0 = "0xabc".into();

        // Failed state query and lost event connection are sticky failures.
        for disconnect in [false, true] {
            reset(&root);
            let guard = desktop.guard().await;
            if disconnect {
                desktop.event.lock().await.take();
                tokio::time::sleep(Duration::from_millis(10)).await;
            } else {
                desktop.snapshot.lock().unwrap().2 = true;
            }
            assert_eq!(
                paste(guard, 1).output_delivery(NOTE).await.unwrap(),
                OutputDelivery::Clipboard
            );
            assert!(read(&root, "keys").is_empty());
            assert!(read(&root, "notices").contains("Destination detection failed"));
            desktop.snapshot.lock().unwrap().2 = false;
        }

        // Failed clipboard delivery cannot fall through to a keyboard driver.
        reset(&root);
        let guard = desktop.guard().await;
        desktop.emit("openlayer>>omarchy-polkit\n").await;
        std::fs::write(root.join("fail-copy"), "").unwrap();
        assert!(paste(guard, 1).output_delivery(NOTE).await.is_err());
        assert!(read(&root, "keys").is_empty());
        assert!(read(&root, "notices").contains("copying dictation to the clipboard failed"));
        assert!(!read(&root, "notices").contains("copied instead"));

        // Dropping a cancelled recording emits nothing and the next starts clean.
        reset(&root);
        let guard = desktop.guard().await;
        desktop.emit("activewindowv2>>def\n").await;
        drop(guard);
        assert!(read(&root, "keys").is_empty());
        assert!(read(&root, "copies").is_empty());
        assert_eq!(desktop.guard().await.check().await, None);
    });
}

#[test]
fn destination_guard_configuration_is_explicit() {
    let mut config = Config::default();
    assert!(!config.output.destination_guard);
    config.output.destination_guard = true;
    assert!(config.validate_destination_guard().is_err());
    config.engine = TranscriptionEngine::Deepgram;
    config.deepgram = Some(DeepgramConfig {
        streaming: true,
        type_partials: false,
        ..Default::default()
    });
    config.output.mode = OutputMode::Paste;
    config.validate_destination_guard().unwrap();
    config.deepgram.as_mut().unwrap().type_partials = true;
    assert!(config.validate_destination_guard().is_err());
    config.deepgram.as_mut().unwrap().type_partials = false;
    config.output.mode = OutputMode::Type;
    assert!(config.validate_destination_guard().is_err());
}

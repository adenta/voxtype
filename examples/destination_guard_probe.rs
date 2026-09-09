//! Manual desktop check: waits for Omarchy authentication, then attempts guarded
//! delivery of harmless sample text. Does not invoke speech recognition.
//! Run only in a test session: the successful check replaces the clipboard.
use std::sync::Arc;
use std::time::Duration;
use voxtype::output::destination_guard::{BlockReason, DestinationGuard};
use voxtype::output::paste::PasteOutput;
use voxtype::output::{OutputDelivery, TextOutput};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let guard = Arc::new(DestinationGuard::start().await);
    anyhow::ensure!(
        guard.check().await.is_none(),
        "Initial destination is not stable"
    );
    voxtype::notification::send(
        "Voxtype test",
        "Checking destination guard; no password entry is needed.",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    anyhow::ensure!(
        guard.check().await.is_none(),
        "An ordinary notification changed the destination"
    );
    println!("Watching for Omarchy authentication (15 seconds)");
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if guard.check().await == Some(BlockReason::AuthenticationPrompt) {
            let paste =
                PasteOutput::new(true, None, Some("shift+insert".into()), 0, 100, true, 1500)
                    .with_destination_guard(Some(guard));
            let result = paste
                .output_delivery("Voxtype destination guard desktop test")
                .await?;
            anyhow::ensure!(
                result == OutputDelivery::Clipboard,
                "Expected clipboard-only output"
            );
            println!("PASS: authentication overlay detected; sample delivered to clipboard");
            return Ok(());
        }
    }
    anyhow::bail!("Authentication overlay was not detected during the test");
}

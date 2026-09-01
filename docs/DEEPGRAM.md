# Deepgram Batch and Streaming Transcription

Voxtype can either send a completed recording to Deepgram's pre-recorded API or stream microphone audio to its live WebSocket API. Deepgram is available in every binary and does not run a model locally, which makes it useful on low-power computers. Recording, notifications, and cursor output remain local Voxtype behavior; only the audio needed for transcription is sent to Deepgram.

## Configuration

```toml
engine = "deepgram"

[deepgram]
model = "nova-3"
language = "en"
streaming = false
type_partials = false
endpointing_ms = 300
smart_format = true
mip_opt_out = true
timeout_secs = 30
endpoint = "https://api.deepgram.com/v1/listen"
```

`language` accepts a BCP-47 language code such as `en`, `en-US`, or `fr`; `auto` enables language detection; and `multi` enables multilingual recognition. `mip_opt_out = true` asks Deepgram to exclude the request from its Model Improvement Program.

`streaming = false` preserves batch push-to-talk behavior. With `streaming = true`, Voxtype streams 16 kHz mono PCM while recording and uses its existing streaming output pipeline. `type_partials = false` types only stable finalized segments; enable it to display revisable interim hypotheses. `endpointing_ms` controls how much silence Deepgram waits for before finalizing an utterance.

The existing HTTPS `endpoint` is also the source for the streaming URL. Voxtype converts `https` to `wss` internally (and loopback `http` to `ws` for tests), so no separate streaming endpoint is needed.

## Credentials

Set `DEEPGRAM_API_KEY` in the daemon environment. It takes precedence over the optional `[deepgram] api_key` fallback:

```bash
export DEEPGRAM_API_KEY="..."
voxtype daemon
```

Avoid putting the key in shell history or a world-readable config. On Linux, a user service can retrieve it from Secret Service immediately before launching Voxtype:

```bash
key="$(secret-tool lookup application voxtype provider deepgram)"
export DEEPGRAM_API_KEY="$key"
exec voxtype daemon
```

The configuration TUI reports whether a credential came from the environment or config, but never displays its value. Voxtype does not log the credential or audio.

## Request behavior

In batch mode, Voxtype encodes the completed 16 kHz mono recording as PCM WAV and sends one HTTPS `POST` with `Content-Type: audio/wav` and `Authorization: Token ...`. The final transcript is processed and inserted at the cursor through the same output path as local engines.

In streaming mode, Voxtype opens an authenticated WebSocket, sends raw 16-bit PCM frames during recording, and consumes interim and finalized results. Stopping sends Deepgram `Finalize` and `CloseStream` control messages, then drains trailing finalized text before returning to idle. Cancelling closes the stream without committing later results.

Authentication, insufficient-credit, rate-limit, network, timeout, empty-result, and malformed-response failures are surfaced as transcription errors. No automatic retry is performed, so a repeated hotkey press cannot accidentally duplicate text.

## Live smoke test

With a key present, record a short sample using the normal hotkey and confirm that the transcript appears at the cursor. Test both `streaming = false` and `streaming = true`. For an isolated batch WAV test, use:

```bash
DEEPGRAM_LIVE_TEST_WAV=/absolute/path/to/short.wav cargo test deepgram_live -- --ignored
```

The live test is ignored in normal CI and also requires `DEEPGRAM_API_KEY`.

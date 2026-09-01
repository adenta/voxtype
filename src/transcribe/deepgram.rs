//! Deepgram pre-recorded and live-streaming speech-to-text backend.

use super::audio::{encode_pcm_s16le, encode_wav_s16le, SAMPLE_RATE};
use super::streaming::{SegmentId, StreamHandle, StreamingEvent, StreamingTranscriber};
use super::Transcriber;
use crate::config::DeepgramConfig;
use crate::error::TranscribeError;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

pub struct DeepgramTranscriber {
    config: DeepgramConfig,
    api_key: String,
}

impl fmt::Debug for DeepgramTranscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeepgramTranscriber")
            .field("endpoint", &self.config.endpoint)
            .field("model", &self.config.model)
            .field("language", &self.config.language)
            .field("streaming", &self.config.streaming)
            .field("api_key", &"***")
            .finish()
    }
}

const FINALIZE_FRAME: &str = r#"{"type":"Finalize"}"#;
const CLOSE_STREAM_FRAME: &str = r#"{"type":"CloseStream"}"#;
const STREAM_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
struct DeepgramResponse {
    results: DeepgramResults,
}

#[derive(Debug, Deserialize)]
struct DeepgramResults {
    channels: Vec<DeepgramChannel>,
}

#[derive(Debug, Deserialize)]
struct DeepgramChannel {
    alternatives: Vec<DeepgramAlternative>,
}

#[derive(Debug, Deserialize)]
struct DeepgramAlternative {
    transcript: String,
}

#[derive(Debug, Deserialize)]
struct DeepgramStreamMessage {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    channel: Option<DeepgramChannel>,
    #[serde(default)]
    is_final: bool,
    #[serde(default)]
    start: f64,
    #[serde(default)]
    err_code: Option<String>,
    #[serde(default)]
    err_msg: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Default)]
struct DeepgramReconciler {
    typed_partial: String,
    has_finalized_text: bool,
    last_final_start_ms: Option<u64>,
    segment_id: SegmentId,
}

#[derive(Debug, Default)]
struct DeepgramFinalBuffer {
    text: String,
    last_final_start_ms: Option<u64>,
}

impl DeepgramFinalBuffer {
    fn push(&mut self, transcript: &str, start: f64) {
        let transcript = transcript.trim();
        if transcript.is_empty() {
            return;
        }

        let start_ms = (start.max(0.0) * 1000.0).round() as u64;
        if self.last_final_start_ms == Some(start_ms) {
            return;
        }
        self.last_final_start_ms = Some(start_ms);

        let punctuation_continuation = transcript.chars().next().is_some_and(|character| {
            matches!(
                character,
                '.' | ',' | '!' | '?' | ';' | ':' | '%' | ')' | ']' | '}'
            )
        });
        if !self.text.is_empty() && !punctuation_continuation {
            self.text.push(' ');
        }
        self.text.push_str(transcript);
    }

    fn take(&mut self) -> String {
        std::mem::take(&mut self.text)
    }
}

impl DeepgramReconciler {
    fn normalize_segment(&self, text: &str) -> String {
        let text = text.trim();
        if text.is_empty() {
            return String::new();
        }
        if self.has_finalized_text {
            format!(" {}", text)
        } else {
            text.to_string()
        }
    }

    fn process(
        &mut self,
        transcript: &str,
        is_final: bool,
        start: f64,
        type_partials: bool,
    ) -> Vec<StreamingEvent> {
        let text = self.normalize_segment(transcript);
        if text.is_empty() {
            return Vec::new();
        }

        if !is_final {
            if !type_partials || !text.starts_with(&self.typed_partial) {
                return Vec::new();
            }
            let delta = text[self.typed_partial.len()..].to_string();
            self.typed_partial = text;
            return if delta.is_empty() {
                Vec::new()
            } else {
                vec![StreamingEvent::Partial {
                    text: delta,
                    segment_id: self.segment_id,
                }]
            };
        }

        let start_ms = (start.max(0.0) * 1000.0).round() as u64;
        if self.last_final_start_ms == Some(start_ms) {
            return Vec::new();
        }
        self.last_final_start_ms = Some(start_ms);

        let event = if self.typed_partial.is_empty() {
            StreamingEvent::Final {
                text,
                segment_id: self.segment_id,
            }
        } else if text.starts_with(&self.typed_partial) {
            StreamingEvent::Final {
                text: text[self.typed_partial.len()..].to_string(),
                segment_id: self.segment_id,
            }
        } else {
            let common = common_prefix_char_count(&self.typed_partial, &text);
            StreamingEvent::Replace {
                backspace: self.typed_partial.chars().count() - common,
                text: text.chars().skip(common).collect(),
                segment_id: self.segment_id,
            }
        };

        self.typed_partial.clear();
        self.has_finalized_text = true;
        self.segment_id = self.segment_id.saturating_add(1);
        vec![event]
    }
}

fn common_prefix_char_count(left: &str, right: &str) -> usize {
    left.chars()
        .zip(right.chars())
        .take_while(|(left, right)| left == right)
        .count()
}

impl DeepgramTranscriber {
    pub fn new(config: DeepgramConfig) -> Result<Self, TranscribeError> {
        validate_config(&config)?;

        let api_key = std::env::var("DEEPGRAM_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty())
            .or_else(|| config.api_key.clone().filter(|key| !key.trim().is_empty()))
            .ok_or_else(|| {
                TranscribeError::ConfigError(
                    "Deepgram API key required: set DEEPGRAM_API_KEY or [deepgram] api_key".into(),
                )
            })?;

        tracing::info!(
            "Deepgram backend configured: endpoint={}, model={}, language={}, streaming={}, type_partials={}, buffered_output={}, smart_format={}, mip_opt_out={}, timeout={}s",
            config.endpoint,
            config.model,
            config.language,
            config.streaming,
            config.type_partials,
            config.streaming && !config.type_partials,
            config.smart_format,
            config.mip_opt_out,
            config.timeout_secs,
        );

        Ok(Self { config, api_key })
    }

    fn websocket_url(&self) -> Result<reqwest::Url, TranscribeError> {
        websocket_url(&self.config)
    }

    fn websocket_request(
        &self,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, TranscribeError> {
        let url = self.websocket_url()?;
        let mut request = url.as_str().into_client_request().map_err(|error| {
            TranscribeError::ConfigError(format!(
                "Failed to construct Deepgram streaming request: {}",
                error
            ))
        })?;
        let mut authorization = HeaderValue::from_str(&format!("Token {}", self.api_key))
            .map_err(|_| TranscribeError::ConfigError("Invalid Deepgram API key".into()))?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);
        Ok(request)
    }

    fn request(&self) -> ureq::Request {
        let mut request = ureq::post(&self.config.endpoint)
            .timeout(Duration::from_secs(self.config.timeout_secs))
            .set("Authorization", &format!("Token {}", self.api_key))
            .set("Content-Type", "audio/wav")
            .query("model", &self.config.model)
            .query(
                "smart_format",
                if self.config.smart_format {
                    "true"
                } else {
                    "false"
                },
            )
            .query(
                "mip_opt_out",
                if self.config.mip_opt_out {
                    "true"
                } else {
                    "false"
                },
            );

        request = match self.config.language.as_str() {
            "auto" => request.query("detect_language", "true"),
            language => request.query("language", language),
        };
        request
    }

    fn parse_response(body: &str) -> Result<String, TranscribeError> {
        let response: DeepgramResponse = serde_json::from_str(body).map_err(|e| {
            TranscribeError::RemoteError(format!("Deepgram returned malformed JSON: {}", e))
        })?;

        let transcript = response
            .results
            .channels
            .first()
            .and_then(|channel| channel.alternatives.first())
            .map(|alternative| alternative.transcript.trim().to_string())
            .ok_or_else(|| {
                TranscribeError::RemoteError(
                    "Deepgram response contained no channel alternatives".into(),
                )
            })?;

        if transcript.is_empty() {
            return Err(TranscribeError::RemoteError(
                "Deepgram returned an empty transcript".into(),
            ));
        }

        Ok(transcript)
    }
}

impl Transcriber for DeepgramTranscriber {
    fn transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        if samples.is_empty() {
            return Err(TranscribeError::AudioFormat("Empty audio buffer".into()));
        }

        let wav = encode_wav_s16le(samples)?;
        let duration_secs = samples.len() as f32 / SAMPLE_RATE as f32;
        tracing::debug!(
            "Sending {:.2}s of audio to Deepgram ({} KiB WAV)",
            duration_secs,
            wav.len() / 1024
        );
        let started = std::time::Instant::now();

        let response = self.request().send_bytes(&wav).map_err(map_request_error)?;
        let body = response.into_string().map_err(|e| {
            TranscribeError::RemoteError(format!("Failed to read Deepgram response: {}", e))
        })?;
        let transcript = Self::parse_response(&body)?;

        tracing::info!(
            "Deepgram transcription completed in {:.2}s",
            started.elapsed().as_secs_f32()
        );
        Ok(transcript)
    }

    fn last_detected_language(&self) -> Option<String> {
        match self.config.language.as_str() {
            "auto" | "multi" => None,
            language => Some(language.to_string()),
        }
    }

    fn as_streaming(&self) -> Option<&dyn StreamingTranscriber> {
        self.config.streaming.then_some(self)
    }
}

impl StreamingTranscriber for DeepgramTranscriber {
    fn start_stream(
        &self,
        samples_rx: mpsc::Receiver<Vec<f32>>,
    ) -> Result<StreamHandle, TranscribeError> {
        let request = self.websocket_request()?;
        let timeout = Duration::from_secs(self.config.timeout_secs);
        let type_partials = self.config.type_partials;
        let (events_tx, events_rx) = mpsc::channel(64);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            run_streaming_session(
                request,
                timeout,
                type_partials,
                samples_rx,
                events_tx,
                cancel_rx,
            )
            .await
        });

        Ok(StreamHandle {
            events: events_rx,
            cancel: cancel_tx,
            task,
        })
    }
}

fn websocket_url(config: &DeepgramConfig) -> Result<reqwest::Url, TranscribeError> {
    let mut url = reqwest::Url::parse(&config.endpoint).map_err(|error| {
        TranscribeError::ConfigError(format!("Invalid Deepgram endpoint URL: {}", error))
    })?;
    let websocket_scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        _ => {
            return Err(TranscribeError::ConfigError(
                "Deepgram endpoint cannot be converted to a WebSocket URL".into(),
            ))
        }
    };
    url.set_scheme(websocket_scheme).map_err(|_| {
        TranscribeError::ConfigError("Failed to derive Deepgram WebSocket endpoint".into())
    })?;

    {
        let mut query = url.query_pairs_mut();
        query.append_pair("model", &config.model);
        query.append_pair(
            "smart_format",
            if config.smart_format { "true" } else { "false" },
        );
        query.append_pair(
            "mip_opt_out",
            if config.mip_opt_out { "true" } else { "false" },
        );
        query.append_pair("encoding", "linear16");
        query.append_pair("sample_rate", &SAMPLE_RATE.to_string());
        query.append_pair("channels", "1");
        query.append_pair("interim_results", "true");
        query.append_pair("endpointing", &config.endpointing_ms.to_string());
        match config.language.as_str() {
            "auto" => {
                query.append_pair("detect_language", "true");
            }
            language => {
                query.append_pair("language", language);
            }
        }
    }
    Ok(url)
}

async fn send_stream_error(events_tx: &mpsc::Sender<StreamingEvent>, error: TranscribeError) {
    let _ = events_tx.send(StreamingEvent::Error(error)).await;
}

async fn run_streaming_session(
    request: tokio_tungstenite::tungstenite::http::Request<()>,
    connect_timeout: Duration,
    type_partials: bool,
    mut samples_rx: mpsc::Receiver<Vec<f32>>,
    events_tx: mpsc::Sender<StreamingEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<(), TranscribeError> {
    let connection =
        tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(request)).await;
    let websocket = match connection {
        Ok(Ok((websocket, _))) => websocket,
        Ok(Err(error)) => {
            send_stream_error(&events_tx, map_websocket_error(error)).await;
            let _ = events_tx.send(StreamingEvent::Ended).await;
            return Ok(());
        }
        Err(_) => {
            send_stream_error(
                &events_tx,
                TranscribeError::NetworkError("Deepgram streaming connection timed out".into()),
            )
            .await;
            let _ = events_tx.send(StreamingEvent::Ended).await;
            return Ok(());
        }
    };

    let (mut write, mut read) = websocket.split();
    let mut reconciler = DeepgramReconciler::default();
    let mut final_buffer = DeepgramFinalBuffer::default();
    let mut samples_closed = false;
    let mut drain_deadline = None;
    let mut completed_normally = false;

    loop {
        let drain_timer = async {
            match drain_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;

            _ = &mut cancel_rx => {
                tracing::debug!("Deepgram streaming session cancelled");
                break;
            }

            _ = drain_timer, if drain_deadline.is_some() => {
                send_stream_error(
                    &events_tx,
                    TranscribeError::NetworkError(
                        "Deepgram streaming finalization timed out after 5 seconds".into(),
                    ),
                ).await;
                break;
            }

            chunk = samples_rx.recv(), if !samples_closed => {
                match chunk {
                    Some(samples) if !samples.is_empty() => {
                        if let Err(error) = write
                            .send(Message::Binary(encode_pcm_s16le(&samples)))
                            .await
                        {
                            send_stream_error(&events_tx, map_websocket_error(error)).await;
                            break;
                        }
                    }
                    Some(_) => {}
                    None => {
                        samples_closed = true;
                        let finalize = write.send(Message::Text(FINALIZE_FRAME.into())).await;
                        let close = if finalize.is_ok() {
                            write.send(Message::Text(CLOSE_STREAM_FRAME.into())).await
                        } else {
                            Ok(())
                        };
                        if let Err(error) = finalize.and(close) {
                            send_stream_error(&events_tx, map_websocket_error(error)).await;
                            break;
                        }
                        drain_deadline = Some(tokio::time::Instant::now() + STREAM_DRAIN_TIMEOUT);
                    }
                }
            }

            incoming = read.next() => {
                let message = match incoming {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        send_stream_error(&events_tx, map_websocket_error(error)).await;
                        break;
                    }
                    None => {
                        if !samples_closed {
                            send_stream_error(
                                &events_tx,
                                TranscribeError::NetworkError(
                                    "Deepgram streaming connection closed unexpectedly".into(),
                                ),
                            ).await;
                        } else {
                            completed_normally = true;
                        }
                        break;
                    }
                };

                match message {
                    Message::Text(text) => {
                        let parsed: DeepgramStreamMessage = match serde_json::from_str(&text) {
                            Ok(parsed) => parsed,
                            Err(error) => {
                                send_stream_error(
                                    &events_tx,
                                    TranscribeError::RemoteError(format!(
                                        "Deepgram returned malformed streaming JSON: {}",
                                        error,
                                    )),
                                ).await;
                                break;
                            }
                        };

                        if parsed.kind == "Error" || parsed.err_code.is_some() || parsed.err_msg.is_some() {
                            let detail = parsed
                                .err_msg
                                .or(parsed.description)
                                .unwrap_or_else(|| "unknown streaming error".into());
                            let code = parsed.err_code.map(|code| format!(" ({})", code)).unwrap_or_default();
                            send_stream_error(
                                &events_tx,
                                TranscribeError::RemoteError(format!("Deepgram streaming error{}: {}", code, detail)),
                            ).await;
                            break;
                        }

                        if parsed.kind == "Results" {
                            let transcript = parsed
                                .channel
                                .as_ref()
                                .and_then(|channel| channel.alternatives.first())
                                .map(|alternative| alternative.transcript.as_str())
                                .unwrap_or_default();
                            if type_partials {
                                for event in reconciler.process(
                                    transcript,
                                    parsed.is_final,
                                    parsed.start,
                                    true,
                                ) {
                                    if events_tx.send(event).await.is_err() {
                                        break;
                                    }
                                }
                            } else if parsed.is_final {
                                final_buffer.push(transcript, parsed.start);
                            }
                        } else if parsed.kind == "Metadata" && samples_closed {
                            completed_normally = true;
                            break;
                        }
                    }
                    Message::Ping(payload) => {
                        if let Err(error) = write.send(Message::Pong(payload)).await {
                            send_stream_error(&events_tx, map_websocket_error(error)).await;
                            break;
                        }
                    }
                    Message::Close(_) => {
                        if !samples_closed {
                            send_stream_error(
                                &events_tx,
                                TranscribeError::NetworkError(
                                    "Deepgram streaming connection closed unexpectedly".into(),
                                ),
                            ).await;
                        } else {
                            completed_normally = true;
                        }
                        break;
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }

    let _ = write.send(Message::Close(None)).await;
    if completed_normally && !type_partials {
        let text = final_buffer.take();
        if !text.is_empty() {
            let _ = events_tx
                .send(StreamingEvent::Final {
                    text,
                    segment_id: 0,
                })
                .await;
        }
    }
    let _ = events_tx.send(StreamingEvent::Ended).await;
    Ok(())
}

fn map_websocket_error(error: tokio_tungstenite::tungstenite::Error) -> TranscribeError {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            let code = response.status().as_u16();
            let summary = match code {
                401 | 403 => "authentication failed",
                402 => "account has insufficient credit",
                429 => "rate limit exceeded",
                _ => "connection failed",
            };
            TranscribeError::RemoteError(format!("Deepgram streaming {} (HTTP {})", summary, code))
        }
        tokio_tungstenite::tungstenite::Error::Io(error) => {
            TranscribeError::NetworkError(format!("Deepgram streaming network error: {}", error))
        }
        error => {
            TranscribeError::NetworkError(format!("Deepgram streaming WebSocket error: {}", error))
        }
    }
}

fn validate_config(config: &DeepgramConfig) -> Result<(), TranscribeError> {
    let endpoint = reqwest::Url::parse(&config.endpoint).map_err(|e| {
        TranscribeError::ConfigError(format!("Invalid Deepgram endpoint URL: {}", e))
    })?;
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(TranscribeError::ConfigError(
            "Deepgram endpoint must not contain credentials".into(),
        ));
    }
    let is_loopback = endpoint.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && is_loopback) {
        return Err(TranscribeError::ConfigError(
            "Deepgram endpoint must use HTTPS (HTTP is allowed only for localhost tests)".into(),
        ));
    }
    if config.model.trim().is_empty() {
        return Err(TranscribeError::ConfigError(
            "Deepgram model must not be empty".into(),
        ));
    }
    if config.language.trim().is_empty() {
        return Err(TranscribeError::ConfigError(
            "Deepgram language must not be empty".into(),
        ));
    }
    if config.timeout_secs == 0 {
        return Err(TranscribeError::ConfigError(
            "Deepgram timeout_secs must be greater than zero".into(),
        ));
    }
    if config.endpointing_ms == 0 || config.endpointing_ms > 10_000 {
        return Err(TranscribeError::ConfigError(
            "Deepgram endpointing_ms must be between 1 and 10000".into(),
        ));
    }
    Ok(())
}

fn map_request_error(error: ureq::Error) -> TranscribeError {
    match error {
        ureq::Error::Status(code, response) => {
            let detail = response.into_string().unwrap_or_default();
            let summary = match code {
                401 | 403 => "authentication failed",
                402 => "account has insufficient credit",
                429 => "rate limit exceeded",
                _ => "request failed",
            };
            if detail.is_empty() {
                TranscribeError::RemoteError(format!("Deepgram {} (HTTP {})", summary, code))
            } else {
                TranscribeError::RemoteError(format!(
                    "Deepgram {} (HTTP {}): {}",
                    summary,
                    code,
                    truncate_detail(&detail)
                ))
            }
        }
        ureq::Error::Transport(error) => {
            TranscribeError::NetworkError(format!("Deepgram request failed: {}", error))
        }
    }
}

fn truncate_detail(detail: &str) -> String {
    const MAX_CHARS: usize = 500;
    let trimmed = detail.trim();
    if trimmed.chars().count() <= MAX_CHARS {
        trimmed.to_string()
    } else {
        format!("{}…", trimmed.chars().take(MAX_CHARS).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn config_with_key() -> DeepgramConfig {
        DeepgramConfig {
            api_key: Some("config-key".into()),
            ..Default::default()
        }
    }

    fn spawn_server(
        status: &str,
        response_body: &str,
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let response_body = response_body.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut headers = [0u8; 4096];
            let read = stream.read(&mut headers).unwrap();
            request.extend_from_slice(&headers[..read]);

            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
                .unwrap();
            let header_text = String::from_utf8_lossy(&request[..header_end]);
            let content_length = header_text
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Content-Length: ")
                        .or_else(|| line.strip_prefix("content-length: "))
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            while request.len() - header_end < content_length {
                let mut chunk = [0u8; 4096];
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
            }

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (format!("http://{}/v1/listen", address), handle)
    }

    #[test]
    fn environment_key_takes_precedence() {
        let _guard = env_lock();
        std::env::set_var("DEEPGRAM_API_KEY", "environment-key");
        let transcriber = DeepgramTranscriber::new(config_with_key()).unwrap();
        std::env::remove_var("DEEPGRAM_API_KEY");
        assert_eq!(transcriber.api_key, "environment-key");
    }

    #[test]
    fn config_key_is_fallback() {
        let _guard = env_lock();
        std::env::remove_var("DEEPGRAM_API_KEY");
        let transcriber = DeepgramTranscriber::new(config_with_key()).unwrap();
        assert_eq!(transcriber.api_key, "config-key");
    }

    #[test]
    fn missing_key_is_rejected() {
        let _guard = env_lock();
        std::env::remove_var("DEEPGRAM_API_KEY");
        let error = DeepgramTranscriber::new(DeepgramConfig::default()).unwrap_err();
        assert!(error.to_string().contains("DEEPGRAM_API_KEY"));
    }

    #[test]
    fn endpoint_requires_https_except_localhost() {
        let mut config = config_with_key();
        config.endpoint = "http://example.com/v1/listen".into();
        let error = DeepgramTranscriber::new(config).unwrap_err();
        assert!(error.to_string().contains("HTTPS"));

        let mut lookalike = config_with_key();
        lookalike.endpoint = "http://localhost.example.com/v1/listen".into();
        assert!(DeepgramTranscriber::new(lookalike).is_err());

        let mut credentials = config_with_key();
        credentials.endpoint = "https://user:password@api.deepgram.com/v1/listen".into();
        assert!(DeepgramTranscriber::new(credentials)
            .unwrap_err()
            .to_string()
            .contains("must not contain credentials"));
    }

    #[test]
    fn streaming_url_is_derived_from_batch_endpoint() {
        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            api_key: Some("key".into()),
            streaming: true,
            language: "auto".into(),
            endpointing_ms: 450,
            ..Default::default()
        })
        .unwrap();
        let url = transcriber.websocket_url().unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.host_str(), Some("api.deepgram.com"));
        let query = url.query().unwrap();
        assert!(query.contains("encoding=linear16"));
        assert!(query.contains("sample_rate=16000"));
        assert!(query.contains("channels=1"));
        assert!(query.contains("interim_results=true"));
        assert!(query.contains("endpointing=450"));
        assert!(query.contains("detect_language=true"));
        assert!(!url.query_pairs().any(|(key, _)| key == "language"));

        let mut local = config_with_key();
        local.endpoint = "http://127.0.0.1:1234/v1/listen".into();
        assert_eq!(websocket_url(&local).unwrap().scheme(), "ws");
    }

    #[test]
    fn streaming_capability_respects_config() {
        let batch = DeepgramTranscriber::new(config_with_key()).unwrap();
        assert!(batch.as_streaming().is_none());

        let streaming = DeepgramTranscriber::new(DeepgramConfig {
            streaming: true,
            ..config_with_key()
        })
        .unwrap();
        assert!(streaming.as_streaming().is_some());
    }

    #[test]
    fn reconciler_spaces_finals_and_suppresses_duplicate_ranges() {
        let mut reconciler = DeepgramReconciler::default();
        let first = reconciler.process("hello", true, 0.0, false);
        let second = reconciler.process("world", true, 1.25, false);
        let duplicate = reconciler.process("world", true, 1.25, false);
        assert!(matches!(
            &first[0],
            StreamingEvent::Final { text, .. } if text == "hello"
        ));
        assert!(matches!(
            &second[0],
            StreamingEvent::Final { text, .. } if text == " world"
        ));
        assert!(duplicate.is_empty());
    }

    #[test]
    fn reconciler_emits_safe_partial_deltas_and_repairs_final_revision() {
        let mut reconciler = DeepgramReconciler::default();
        let first = reconciler.process("hel", false, 0.0, true);
        let extension = reconciler.process("hello", false, 0.0, true);
        let revision = reconciler.process("help", false, 0.0, true);
        let final_event = reconciler.process("help", true, 0.0, true);
        assert!(matches!(
            &first[0],
            StreamingEvent::Partial { text, .. } if text == "hel"
        ));
        assert!(matches!(
            &extension[0],
            StreamingEvent::Partial { text, .. } if text == "lo"
        ));
        assert!(revision.is_empty());
        assert!(matches!(
            &final_event[0],
            StreamingEvent::Replace { backspace: 2, text, .. } if text == "p"
        ));
    }

    #[test]
    fn final_buffer_joins_segments_and_suppresses_duplicate_ranges() {
        let mut buffer = DeepgramFinalBuffer::default();
        buffer.push("The quick brown fox", 0.0);
        buffer.push("jumped over the lazy dog", 1.25);
        buffer.push("jumped over the lazy dog", 1.25);
        buffer.push(", then rested.", 2.5);
        assert_eq!(
            buffer.take(),
            "The quick brown fox jumped over the lazy dog, then rested."
        );
    }

    #[test]
    fn parses_transcript_and_rejects_empty_transcript() {
        let body =
            r#"{"results":{"channels":[{"alternatives":[{"transcript":" hello world "}]}]}}"#;
        assert_eq!(
            DeepgramTranscriber::parse_response(body).unwrap(),
            "hello world"
        );

        let empty = r#"{"results":{"channels":[{"alternatives":[{"transcript":""}]}]}}"#;
        assert!(DeepgramTranscriber::parse_response(empty)
            .unwrap_err()
            .to_string()
            .contains("empty transcript"));
    }

    #[test]
    fn malformed_and_missing_results_are_rejected() {
        assert!(DeepgramTranscriber::parse_response("not json").is_err());
        assert!(DeepgramTranscriber::parse_response(
            r#"{"results":{"channels":[{"alternatives":[]}]}}"#
        )
        .is_err());
    }

    #[test]
    fn request_contains_expected_headers_query_and_wav() {
        let body = r#"{"results":{"channels":[{"alternatives":[{"transcript":"test"}]}]}}"#;
        let (endpoint, server) = spawn_server("200 OK", body);
        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("secret-test-key".into()),
            language: "auto".into(),
            ..Default::default()
        })
        .unwrap();

        let transcript = transcriber.transcribe(&[0.0; 160]).unwrap();
        assert_eq!(transcript, "test");
        let request = server.join().unwrap();
        assert!(request.starts_with("POST /v1/listen?"));
        assert!(request.contains("model=nova-3"));
        assert!(request.contains("smart_format=true"));
        assert!(request.contains("mip_opt_out=true"));
        assert!(request.contains("detect_language=true"));
        assert!(!request.contains("&language="));
        assert!(request
            .to_ascii_lowercase()
            .contains("content-type: audio/wav"));
        assert!(request.contains("Authorization: Token secret-test-key"));
        assert!(request.contains("RIFF"));
        assert!(request.contains("WAVE"));
    }

    #[test]
    fn http_errors_have_actionable_categories() {
        let (endpoint, server) =
            spawn_server("429 Too Many Requests", r#"{"err_code":"TooManyRequests"}"#);
        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("key".into()),
            ..Default::default()
        })
        .unwrap();
        let error = transcriber.transcribe(&[0.0; 16]).unwrap_err();
        server.join().unwrap();
        assert!(error.to_string().contains("rate limit exceeded"));
        assert!(error.to_string().contains("429"));
    }

    #[test]
    fn request_timeout_is_a_network_error() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/listen", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_secs(2));
        });
        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("key".into()),
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();

        let error = transcriber.transcribe(&[0.0; 16]).unwrap_err();
        server.join().unwrap();
        assert!(matches!(error, TranscribeError::NetworkError(_)));
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_stream_sends_auth_query_pcm_and_shutdown_frames() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/listen", listener.local_addr().unwrap());
        let request_capture = Arc::new(Mutex::new(String::new()));
        let server_capture = request_capture.clone();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let capture = server_capture.clone();
            let mut websocket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    let authorization = request
                        .headers()
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    *capture.lock().unwrap() =
                        format!("{}\n{}", request.uri(), authorization);
                    Ok(response)
                },
            )
            .await
            .unwrap();

            let mut pcm = None;
            let mut saw_finalize = false;
            let mut saw_close_stream = false;
            while let Some(message) = websocket.next().await {
                match message.unwrap() {
                    Message::Binary(bytes) => {
                        pcm = Some(bytes.to_vec());
                        websocket.send(Message::Text(
                            r#"{"type":"Results","start":0.0,"is_final":true,"channel":{"alternatives":[{"transcript":"hello"}]}}"#.into()
                        )).await.unwrap();
                    }
                    Message::Text(text) if text.as_str() == FINALIZE_FRAME => {
                        saw_finalize = true;
                    }
                    Message::Text(text) if text.as_str() == CLOSE_STREAM_FRAME => {
                        saw_close_stream = true;
                        websocket.send(Message::Text(
                            r#"{"type":"Results","start":1.0,"is_final":true,"channel":{"alternatives":[{"transcript":"world"}]}}"#.into()
                        )).await.unwrap();
                        websocket
                            .send(Message::Text(
                                r#"{"type":"Metadata","request_id":"test"}"#.into(),
                            ))
                            .await
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
            }
            (pcm.unwrap(), saw_finalize, saw_close_stream)
        });

        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("stream-secret".into()),
            streaming: true,
            ..Default::default()
        })
        .unwrap();
        let (samples_tx, samples_rx) = mpsc::channel(4);
        let StreamHandle {
            mut events,
            cancel,
            task,
        } = transcriber.start_stream(samples_rx).unwrap();
        samples_tx.send(vec![-1.0, 0.0, 1.0]).await.unwrap();

        // Deepgram may finalize segments while the microphone is still open,
        // but finalized-only mode deliberately commits nothing to Voxtype's
        // output pipeline until the user stops recording.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err()
        );
        drop(samples_tx);

        let mut finals = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .unwrap()
                .unwrap();
            match event {
                StreamingEvent::Final { text, .. } => finals.push(text),
                StreamingEvent::Ended => break,
                StreamingEvent::Error(error) => panic!("unexpected streaming error: {error}"),
                _ => {}
            }
        }
        task.await.unwrap().unwrap();
        drop(cancel);
        let (pcm, saw_finalize, saw_close_stream) = server.await.unwrap();

        assert_eq!(pcm, encode_pcm_s16le(&[-1.0, 0.0, 1.0]));
        assert!(saw_finalize);
        assert!(saw_close_stream);
        assert_eq!(finals, vec!["hello world"]);
        let request = request_capture.lock().unwrap().clone();
        assert!(request.contains("model=nova-3"));
        assert!(request.contains("encoding=linear16"));
        assert!(request.contains("endpointing=300"));
        assert!(request.contains("Token stream-secret"));
    }

    #[test]
    fn websocket_http_errors_keep_actionable_categories() {
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(429)
            .body(Some(Vec::new()))
            .unwrap();
        let error = map_websocket_error(tokio_tungstenite::tungstenite::Error::Http(response));
        assert!(error.to_string().contains("rate limit exceeded"));
        assert!(error.to_string().contains("429"));
    }

    #[tokio::test]
    async fn malformed_streaming_json_emits_error_then_ended() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/listen", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            websocket
                .send(Message::Text("not json".into()))
                .await
                .unwrap();
            let _ = websocket.next().await;
        });

        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("key".into()),
            streaming: true,
            ..Default::default()
        })
        .unwrap();
        let (samples_tx, samples_rx) = mpsc::channel(1);
        let StreamHandle {
            mut events,
            cancel,
            task,
        } = transcriber.start_stream(samples_rx).unwrap();

        let error = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            error,
            StreamingEvent::Error(TranscribeError::RemoteError(_))
        ));
        assert!(matches!(
            events.recv().await.unwrap(),
            StreamingEvent::Ended
        ));
        task.await.unwrap().unwrap();
        drop((samples_tx, cancel));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_closes_without_transcript_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/listen", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(message) = websocket.next().await {
                if matches!(message.unwrap(), Message::Close(_)) {
                    break;
                }
            }
        });

        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("key".into()),
            streaming: true,
            ..Default::default()
        })
        .unwrap();
        let (samples_tx, samples_rx) = mpsc::channel(1);
        let StreamHandle {
            mut events,
            cancel,
            task,
        } = transcriber.start_stream(samples_rx).unwrap();
        cancel.send(()).unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap(),
            StreamingEvent::Ended
        ));
        assert!(events.recv().await.is_none());
        task.await.unwrap().unwrap();
        drop(samples_tx);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn streaming_connect_timeout_emits_error_then_ended() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/listen", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let transcriber = DeepgramTranscriber::new(DeepgramConfig {
            endpoint,
            api_key: Some("key".into()),
            streaming: true,
            ..Default::default()
        })
        .unwrap();
        let request = transcriber.websocket_request().unwrap();
        let (_samples_tx, samples_rx) = mpsc::channel(1);
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let (_cancel_tx, cancel_rx) = oneshot::channel();

        run_streaming_session(
            request,
            Duration::from_millis(25),
            false,
            samples_rx,
            events_tx,
            cancel_rx,
        )
        .await
        .unwrap();
        assert!(matches!(
            events_rx.recv().await.unwrap(),
            StreamingEvent::Error(TranscribeError::NetworkError(_))
        ));
        assert!(matches!(
            events_rx.recv().await.unwrap(),
            StreamingEvent::Ended
        ));
        server.abort();
    }

    #[test]
    #[ignore = "requires DEEPGRAM_API_KEY and DEEPGRAM_LIVE_TEST_WAV"]
    fn deepgram_live() {
        let path = std::env::var("DEEPGRAM_LIVE_TEST_WAV")
            .expect("set DEEPGRAM_LIVE_TEST_WAV to a short 16 kHz mono PCM WAV");
        let mut reader = hound::WavReader::open(path).expect("open live-test WAV");
        let spec = reader.spec();
        assert_eq!(spec.channels, 1, "live-test WAV must be mono");
        assert_eq!(
            spec.sample_rate, SAMPLE_RATE,
            "live-test WAV must be 16 kHz"
        );
        assert_eq!(spec.bits_per_sample, 16, "live-test WAV must be 16-bit PCM");
        let samples = reader
            .samples::<i16>()
            .map(|sample| sample.expect("decode live-test sample") as f32 / i16::MAX as f32)
            .collect::<Vec<_>>();

        let transcript = DeepgramTranscriber::new(DeepgramConfig::default())
            .expect("configure Deepgram from DEEPGRAM_API_KEY")
            .transcribe(&samples)
            .expect("live Deepgram transcription");
        assert!(!transcript.trim().is_empty());
    }
}

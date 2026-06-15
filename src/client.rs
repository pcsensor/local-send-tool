use async_compression::tokio::bufread::ZstdEncoder;
use clap::ValueEnum;
use futures_util::stream::{FuturesUnordered, StreamExt};
use reqwest::{
    multipart::{Form, Part},
    Body, Client,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{
    collections::HashSet,
    error::Error,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, ReadBuf},
    task::JoinSet,
};
use tokio_util::io::ReaderStream;

type DynError = Box<dyn Error + Send + Sync>;
type ChunkUploadFuture = Pin<Box<dyn Future<Output = Result<u64, DynError>> + Send>>;

#[derive(Serialize)]
struct MessagePayload<'a> {
    sender_name: &'a str,
    text: &'a str,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionMode {
    #[default]
    Auto,
    Always,
    Never,
}

impl std::fmt::Display for CompressionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionMode::Auto => write!(f, "auto"),
            CompressionMode::Always => write!(f, "always"),
            CompressionMode::Never => write!(f, "never"),
        }
    }
}

impl std::str::FromStr for CompressionMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(CompressionMode::Auto),
            "always" => Ok(CompressionMode::Always),
            "never" => Ok(CompressionMode::Never),
            other => Err(format!(
                "Invalid compression mode '{}'; expected auto, always, or never",
                other
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransferProgress {
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub elapsed: Duration,
    pub bytes_per_second: f64,
}

#[derive(Clone, Default)]
pub enum ProgressMode {
    #[default]
    None,
    Indicatif,
    Callback(Arc<dyn Fn(TransferProgress) + Send + Sync>),
}

#[derive(Clone)]
pub struct FileSendOptions {
    pub retry_attempts: usize,
    pub compression: CompressionMode,
    pub progress: ProgressMode,
    pub connect_timeout: Duration,
    pub cancel_timeout: Duration,
    pub use_chunked: bool,
    pub chunk_size: u64,
    pub chunk_concurrency: usize,
    pub resume_upload_id: Option<String>,
}

impl Default for FileSendOptions {
    fn default() -> Self {
        Self {
            retry_attempts: 0,
            compression: CompressionMode::Auto,
            progress: ProgressMode::None,
            connect_timeout: Duration::from_secs(5),
            cancel_timeout: Duration::from_secs(10),
            use_chunked: false,
            chunk_size: 8 * 1024 * 1024,
            chunk_concurrency: 4,
            resume_upload_id: None,
        }
    }
}

#[derive(Serialize)]
struct InitUploadRequest {
    sender_name: String,
    file_name: String,
    file_size: u64,
    checksum: String,
    chunk_size: u64,
    upload_id: Option<String>,
}

#[derive(Deserialize)]
struct InitUploadResponse {
    upload_id: String,
    received_chunks: Vec<u64>,
    received_bytes: u64,
}

#[derive(Clone)]
struct ProgressSink {
    mode: ProgressMode,
    bar: Option<indicatif::ProgressBar>,
    started_at: Instant,
    initial_bytes: u64,
    chunk_progress: Arc<std::sync::Mutex<std::collections::HashMap<u64, u64>>>,
}

impl ProgressSink {
    fn new(mode: ProgressMode, total_bytes: u64) -> Self {
        Self::new_with_initial(mode, total_bytes, 0)
    }

    fn new_with_initial(mode: ProgressMode, total_bytes: u64, initial_bytes: u64) -> Self {
        let bar = match &mode {
            ProgressMode::Indicatif => {
                let bar = indicatif::ProgressBar::new(total_bytes);
                if let Ok(style) = indicatif::ProgressStyle::with_template(
                    "{bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta}",
                ) {
                    bar.set_style(style.progress_chars("=> "));
                }
                bar.set_position(initial_bytes);
                Some(bar)
            }
            _ => None,
        };
        Self {
            mode,
            bar,
            started_at: Instant::now(),
            initial_bytes,
            chunk_progress: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn report_chunk(&self, chunk_index: u64, sent_in_chunk: u64, total_bytes: u64) {
        let mut progress = self.chunk_progress.lock().unwrap();
        progress.insert(chunk_index, sent_in_chunk);
        let sum_chunks: u64 = progress.values().sum();
        let total_sent = sum_chunks + self.initial_bytes;
        self.report(total_sent.min(total_bytes), total_bytes);
    }

    fn report(&self, sent_bytes: u64, total_bytes: u64) {
        if let Some(bar) = &self.bar {
            bar.set_position(sent_bytes);
        }
        if let ProgressMode::Callback(callback) = &self.mode {
            let elapsed = self.started_at.elapsed();
            let bytes_per_second = if elapsed.is_zero() {
                0.0
            } else {
                sent_bytes as f64 / elapsed.as_secs_f64()
            };
            callback(TransferProgress {
                sent_bytes,
                total_bytes,
                elapsed,
                bytes_per_second,
            });
        }
    }

    fn finish(&self, total_bytes: u64) {
        self.report(total_bytes, total_bytes);
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
        }
    }
}

struct ProgressReader<R> {
    inner: R,
    sink: ProgressSink,
    sent_bytes: u64,
    total_bytes: u64,
    chunk_index: u64,
}

#[derive(Clone)]
struct ChunkUploadRequest {
    client: Client,
    to_addr: String,
    upload_id: String,
    file_path: PathBuf,
    total_bytes: u64,
    chunk_size: u64,
    retry_attempts: usize,
    sink: ProgressSink,
}

struct ChunkedUploadCancelGuard {
    client: Client,
    to_addr: String,
    upload_id: String,
    completed: bool,
}

impl ChunkedUploadCancelGuard {
    fn new(client: Client, to_addr: String, upload_id: String) -> Self {
        Self {
            client,
            to_addr,
            upload_id,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for ChunkedUploadCancelGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let client = self.client.clone();
        let url = format_url(
            &self.to_addr,
            &format!("/api/file/cancel/{}", self.upload_id),
        );
        tokio::spawn(async move {
            let _ = client.delete(url).send().await;
        });
    }
}

impl ChunkUploadRequest {
    async fn upload_with_retry(&self, index: u64) -> Result<u64, DynError> {
        let mut attempt = 0;
        loop {
            match self.upload_once(index).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) if attempt < self.retry_attempts => {
                    let delay = retry_delay(attempt);
                    eprintln!(
                        "Chunk {} attempt {} failed: {}. Retrying in {:?}...",
                        index,
                        attempt + 1,
                        err,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn upload_once(&self, index: u64) -> Result<u64, DynError> {
        let offset = index * self.chunk_size;
        let chunk_bytes = (self.total_bytes - offset).min(self.chunk_size);
        let mut file = File::open(&self.file_path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let reader = file.take(chunk_bytes);
        // 包装成 ProgressReader
        let progress_reader =
            ProgressReader::with_chunk_index(reader, self.sink.clone(), self.total_bytes, index);
        let stream = ReaderStream::with_capacity(progress_reader, 64 * 1024);
        let body = Body::wrap_stream(stream);
        let url = format_url(
            &self.to_addr,
            &format!("/api/file/chunk/{}/{}", self.upload_id, index),
        );

        self.client
            .put(url)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(chunk_bytes)
    }
}

impl<R> ProgressReader<R> {
    fn new(inner: R, sink: ProgressSink, total_bytes: u64) -> Self {
        Self {
            inner,
            sink,
            sent_bytes: 0,
            total_bytes,
            chunk_index: 0,
        }
    }

    fn with_chunk_index(inner: R, sink: ProgressSink, total_bytes: u64, chunk_index: u64) -> Self {
        Self {
            inner,
            sink,
            sent_bytes: 0,
            total_bytes,
            chunk_index,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let after = buf.filled().len();
            let read = after.saturating_sub(before) as u64;
            if read > 0 {
                self.sent_bytes += read;
                self.sink
                    .report_chunk(self.chunk_index, self.sent_bytes, self.total_bytes);
            }
        }
        poll
    }
}

fn format_url(to_addr: &str, path: &str) -> String {
    let base = if to_addr.starts_with("http://") || to_addr.starts_with("https://") {
        to_addr.to_string()
    } else {
        format!("http://{}", to_addr)
    };
    let base = base.trim_end_matches('/');
    format!("{}{}", base, path)
}

pub async fn send_text(to_addr: &str, sender_name: &str, text: &str) -> Result<(), reqwest::Error> {
    let url = format_url(to_addr, "/api/message");
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let payload = MessagePayload { sender_name, text };

    let response = client.post(&url).json(&payload).send().await?;

    response.error_for_status()?;
    Ok(())
}

pub async fn send_file(to_addr: &str, sender_name: &str, file_path: &Path) -> Result<(), DynError> {
    send_file_with_options(to_addr, sender_name, file_path, FileSendOptions::default()).await
}

pub async fn send_file_with_options(
    to_addr: &str,
    sender_name: &str,
    file_path: &Path,
    options: FileSendOptions,
) -> Result<(), DynError> {
    let url = format_url(to_addr, "/api/file");
    let client = Client::builder()
        .connect_timeout(options.connect_timeout)
        .build()?;

    let metadata = tokio::fs::metadata(file_path).await?;
    let total_bytes = metadata.len();
    let checksum = sha256_file_with_progress(file_path, &options.progress).await?;

    if options.use_chunked {
        return send_file_chunked(
            &client,
            to_addr,
            sender_name,
            file_path,
            &options,
            &checksum,
            total_bytes,
        )
        .await;
    }

    let mut attempt = 0;
    loop {
        match send_file_once(
            &client,
            &url,
            sender_name,
            file_path,
            &options,
            &checksum,
            total_bytes,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) if attempt < options.retry_attempts => {
                let delay = retry_delay(attempt);
                eprintln!(
                    "Upload attempt {} failed: {}. Retrying in {:?}...",
                    attempt + 1,
                    err,
                    delay
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn send_file_chunked(
    client: &Client,
    to_addr: &str,
    sender_name: &str,
    file_path: &Path,
    options: &FileSendOptions,
    checksum: &str,
    total_bytes: u64,
) -> Result<(), DynError> {
    let chunk_size = options.chunk_size.max(1);
    let init_url = format_url(to_addr, "/api/file/init");
    let file_name = file_name_string(file_path)?;
    let init_response: InitUploadResponse = client
        .post(init_url)
        .json(&InitUploadRequest {
            sender_name: sender_name.to_string(),
            file_name,
            file_size: total_bytes,
            checksum: checksum.to_string(),
            chunk_size,
            upload_id: options.resume_upload_id.clone(),
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let received_chunks: HashSet<u64> = init_response.received_chunks.into_iter().collect();
    println!("Chunked upload id: {}", init_response.upload_id);
    let mut cancel_guard = ChunkedUploadCancelGuard::new(
        client.clone(),
        to_addr.to_string(),
        init_response.upload_id.clone(),
    );
    let chunk_count = total_bytes.div_ceil(chunk_size);
    let progress = ProgressSink::new_with_initial(
        options.progress.clone(),
        total_bytes,
        init_response.received_bytes,
    );
    progress.report(init_response.received_bytes, total_bytes);

    let mut pending_chunks = (0..chunk_count).filter(|index| !received_chunks.contains(index));
    let concurrency = options.chunk_concurrency.max(1);
    let mut uploads: FuturesUnordered<ChunkUploadFuture> = FuturesUnordered::new();

    for _ in 0..concurrency {
        let Some(index) = pending_chunks.next() else {
            break;
        };
        let upload = ChunkUploadRequest {
            client: client.clone(),
            to_addr: to_addr.to_string(),
            upload_id: init_response.upload_id.clone(),
            file_path: file_path.to_path_buf(),
            total_bytes,
            chunk_size,
            retry_attempts: options.retry_attempts,
            sink: progress.clone(),
        };
        uploads.push(Box::pin(
            async move { upload.upload_with_retry(index).await },
        ));
    }

    while let Some(result) = uploads.next().await {
        result?;

        let Some(index) = pending_chunks.next() else {
            continue;
        };
        let upload = ChunkUploadRequest {
            client: client.clone(),
            to_addr: to_addr.to_string(),
            upload_id: init_response.upload_id.clone(),
            file_path: file_path.to_path_buf(),
            total_bytes,
            chunk_size,
            retry_attempts: options.retry_attempts,
            sink: progress.clone(),
        };
        uploads.push(Box::pin(
            async move { upload.upload_with_retry(index).await },
        ));
    }

    if let ProgressMode::Indicatif = options.progress {
        println!("Verifying file integrity on target server...");
    }

    let complete_url = format_url(
        to_addr,
        &format!("/api/file/complete/{}", init_response.upload_id),
    );
    client.post(complete_url).send().await?.error_for_status()?;
    cancel_guard.complete();
    progress.finish(total_bytes);
    Ok(())
}

pub async fn send_files(
    to_addr: &str,
    sender_name: &str,
    files: &[PathBuf],
    concurrency: usize,
    options: FileSendOptions,
) -> Result<(), DynError> {
    let mut pending_files = files.iter().cloned();
    let mut uploads = JoinSet::new();
    let concurrency = concurrency.max(1);

    for _ in 0..concurrency {
        let Some(file_path) = pending_files.next() else {
            break;
        };
        spawn_file_upload(
            &mut uploads,
            to_addr,
            sender_name,
            file_path,
            options.clone(),
        );
    }

    while let Some(result) = uploads.join_next().await {
        match result {
            Ok(Ok(())) => {
                if let Some(file_path) = pending_files.next() {
                    spawn_file_upload(
                        &mut uploads,
                        to_addr,
                        sender_name,
                        file_path,
                        options.clone(),
                    );
                }
            }
            Ok(Err(err)) => {
                abort_remaining_uploads(&mut uploads).await;
                return Err(err);
            }
            Err(err) => {
                abort_remaining_uploads(&mut uploads).await;
                return Err(Box::new(err));
            }
        }
    }

    Ok(())
}

fn spawn_file_upload(
    uploads: &mut JoinSet<Result<(), DynError>>,
    to_addr: &str,
    sender_name: &str,
    file_path: PathBuf,
    options: FileSendOptions,
) {
    let to_addr = to_addr.to_string();
    let sender_name = sender_name.to_string();
    uploads.spawn(async move {
        send_file_with_options(&to_addr, &sender_name, &file_path, options).await
    });
}

async fn abort_remaining_uploads(uploads: &mut JoinSet<Result<(), DynError>>) {
    uploads.abort_all();
    while uploads.join_next().await.is_some() {}
}

async fn send_file_once(
    client: &Client,
    url: &str,
    sender_name: &str,
    file_path: &Path,
    options: &FileSendOptions,
    checksum: &str,
    total_bytes: u64,
) -> Result<(), DynError> {
    let compressed = should_compress(file_path, options.compression);
    let file_encoding = if compressed { "zstd" } else { "none" };
    let sink = ProgressSink::new(options.progress.clone(), total_bytes);
    let part = build_file_part(file_path, compressed, sink.clone(), total_bytes).await?;

    let form = Form::new()
        .text("sender_name", sender_name.to_string())
        .text("checksum_mode", "sha256")
        .text("checksum", checksum.to_string())
        .text("file_encoding", file_encoding)
        .part("file", part);

    let response = client.post(url).multipart(form).send().await?;
    response.error_for_status()?;
    sink.finish(total_bytes);
    Ok(())
}

async fn build_file_part(
    file_path: &Path,
    compressed: bool,
    sink: ProgressSink,
    total_bytes: u64,
) -> Result<Part, DynError> {
    let file = File::open(file_path).await?;
    let progress_reader = ProgressReader::new(file, sink, total_bytes);
    let reader: Pin<Box<dyn AsyncRead + Send>> = if compressed {
        Box::pin(ZstdEncoder::new(BufReader::new(progress_reader)))
    } else {
        Box::pin(progress_reader)
    };
    let stream = ReaderStream::with_capacity(reader, 64 * 1024);
    let body = Body::wrap_stream(stream);
    let filename = file_name_string(file_path)?;
    let mime = mime_guess::from_path(file_path).first_or_octet_stream();

    Ok(Part::stream(body)
        .file_name(filename)
        .mime_str(mime.as_ref())?)
}

fn file_name_string(file_path: &Path) -> Result<String, DynError> {
    Ok(file_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Invalid file name: {}", file_path.display()))?
        .to_string())
}

pub async fn sha256_file(file_path: &Path) -> Result<String, DynError> {
    sha256_file_with_progress(file_path, &ProgressMode::None).await
}

pub async fn sha256_file_with_progress(
    file_path: &Path,
    progress_mode: &ProgressMode,
) -> Result<String, DynError> {
    let mut file = File::open(file_path).await?;
    let metadata = file.metadata().await?;
    let total_bytes = metadata.len();

    let bar = match progress_mode {
        ProgressMode::Indicatif => {
            let bar = indicatif::ProgressBar::new(total_bytes);
            if let Ok(style) = indicatif::ProgressStyle::with_template(
                "{bar:40.cyan/blue} {bytes}/{total_bytes} Hashing...",
            ) {
                bar.set_style(style.progress_chars("=> "));
            }
            Some(bar)
        }
        _ => None,
    };

    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 256 * 1024]; // 256KB 较大缓冲区以加快计算速度
    let mut hashed_bytes = 0;

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        hashed_bytes += read as u64;
        if let Some(bar) = &bar {
            bar.set_position(hashed_bytes);
        }
    }

    if let Some(bar) = bar {
        bar.finish_and_clear();
    }

    Ok(format!("{:x}", hasher.finalize()))
}

pub fn should_compress(file_path: &Path, mode: CompressionMode) -> bool {
    match mode {
        CompressionMode::Always => true,
        CompressionMode::Never => false,
        CompressionMode::Auto => {
            let mime = mime_guess::from_path(file_path).first_or_octet_stream();
            if mime.type_() == mime_guess::mime::TEXT {
                return true;
            }
            matches!(
                mime.essence_str(),
                "application/json"
                    | "application/javascript"
                    | "application/xml"
                    | "application/x-sh"
                    | "image/svg+xml"
            ) || file_path
                .extension()
                .and_then(|extension| extension.to_str())
                .map(|extension| {
                    matches!(
                        extension.to_ascii_lowercase().as_str(),
                        "csv" | "log" | "md" | "rs" | "toml" | "txt" | "yaml" | "yml"
                    )
                })
                .unwrap_or(false)
        }
    }
}

fn retry_delay(attempt: usize) -> Duration {
    const BACKOFF_MS: [u64; 4] = [100, 500, 2_000, 5_000];
    Duration::from_millis(BACKOFF_MS[attempt.min(BACKOFF_MS.len() - 1)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use crate::server::make_router;
    use axum::{
        extract::{Multipart, Path as AxumPath},
        http::StatusCode,
        routing::{delete, post, put},
        Json, Router,
    };
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    };
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_client_send_text_and_file() {
        let registry = PeerRegistry::new();
        let download_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let router = make_router(registry, download_dir.path().to_path_buf());

        // 启动测试服务器
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server_addr = format!("{}", local_addr);

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        // 1. 测试发送文本
        send_text(&server_addr, "client-sender", "Hello from client!")
            .await
            .unwrap();

        // 2. 测试发送文件
        let test_file_path = source_dir.path().join("upload_test.txt");
        tokio::fs::write(&test_file_path, "Client file content")
            .await
            .unwrap();

        send_file(&server_addr, "client-sender", &test_file_path)
            .await
            .unwrap();

        // 检查服务器端是否保存了文件
        let saved_path = download_dir.path().join("upload_test.txt");
        assert!(saved_path.exists());
        let content = tokio::fs::read_to_string(saved_path).await.unwrap();
        assert_eq!(content, "Client file content");
    }

    #[tokio::test]
    async fn test_send_file_reports_progress_and_compresses_when_requested() {
        let registry = PeerRegistry::new();
        let download_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let router = make_router(registry, download_dir.path().to_path_buf());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let file_path = source_dir.path().join("compressible.txt");
        let content = "progress and compression\n".repeat(1024);
        tokio::fs::write(&file_path, &content).await.unwrap();

        let last_progress = Arc::new(AtomicU64::new(0));
        let progress_updates = Arc::new(AtomicUsize::new(0));
        let options = FileSendOptions {
            compression: CompressionMode::Always,
            progress: ProgressMode::Callback(Arc::new({
                let last_progress = Arc::clone(&last_progress);
                let progress_updates = Arc::clone(&progress_updates);
                move |progress| {
                    last_progress.store(progress.sent_bytes, Ordering::SeqCst);
                    progress_updates.fetch_add(1, Ordering::SeqCst);
                }
            })),
            ..FileSendOptions::default()
        };

        send_file_with_options(&server_addr, "client-sender", &file_path, options)
            .await
            .unwrap();

        assert_eq!(last_progress.load(Ordering::SeqCst), content.len() as u64);
        assert!(progress_updates.load(Ordering::SeqCst) > 0);
        let saved_path = download_dir.path().join("compressible.txt");
        let saved = tokio::fs::read_to_string(saved_path).await.unwrap();
        assert_eq!(saved, content);
    }

    #[tokio::test]
    async fn test_send_file_retries_failed_upload() {
        let source_dir = tempdir().unwrap();
        let file_path = source_dir.path().join("retry.txt");
        tokio::fs::write(&file_path, "retry body").await.unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/api/file",
            post({
                let attempts = Arc::clone(&attempts);
                move |mut multipart: Multipart| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            while let Ok(Some(field)) = multipart.next_field().await {
                                let _ = field.bytes().await;
                            }
                            StatusCode::OK
                        }
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let options = FileSendOptions {
            retry_attempts: 1,
            ..FileSendOptions::default()
        };
        send_file_with_options(&server_addr, "client-sender", &file_path, options)
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_chunked_upload_abort_notifies_receiver_to_cancel() {
        let source_dir = tempdir().unwrap();
        let file_path = source_dir.path().join("abort.bin");
        tokio::fs::write(&file_path, vec![b'x'; 128 * 1024])
            .await
            .unwrap();

        let init_seen = Arc::new(AtomicUsize::new(0));
        let cancel_seen = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/file/init",
                post({
                    let init_seen = Arc::clone(&init_seen);
                    move || {
                        let init_seen = Arc::clone(&init_seen);
                        async move {
                            init_seen.fetch_add(1, Ordering::SeqCst);
                            Json(json!({
                                "upload_id": "abort-upload",
                                "received_chunks": [],
                                "received_bytes": 0,
                            }))
                        }
                    }
                }),
            )
            .route(
                "/api/file/chunk/:upload_id/:index",
                put(|| async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    StatusCode::OK
                }),
            )
            .route(
                "/api/file/cancel/:upload_id",
                delete({
                    let cancel_seen = Arc::clone(&cancel_seen);
                    move |AxumPath(upload_id): AxumPath<String>| {
                        let cancel_seen = Arc::clone(&cancel_seen);
                        async move {
                            assert_eq!(upload_id, "abort-upload");
                            cancel_seen.fetch_add(1, Ordering::SeqCst);
                            StatusCode::OK
                        }
                    }
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let options = FileSendOptions {
            use_chunked: true,
            chunk_size: 64 * 1024,
            chunk_concurrency: 1,
            ..FileSendOptions::default()
        };
        let upload = tokio::spawn({
            let file_path = file_path.clone();
            async move {
                send_file_with_options(&server_addr, "client-sender", &file_path, options).await
            }
        });

        while init_seen.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        upload.abort();

        for _ in 0..100 {
            if cancel_seen.load(Ordering::SeqCst) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(cancel_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_chunked_upload_abort_stops_in_flight_chunk_retries() {
        let source_dir = tempdir().unwrap();
        let file_path = source_dir.path().join("abort-retries.bin");
        tokio::fs::write(&file_path, vec![b'x'; 64 * 1024])
            .await
            .unwrap();

        let chunk_attempts = Arc::new(AtomicUsize::new(0));
        let cancel_seen = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/file/init",
                post(|| async {
                    Json(json!({
                        "upload_id": "abort-retries-upload",
                        "received_chunks": [],
                        "received_bytes": 0,
                    }))
                }),
            )
            .route(
                "/api/file/chunk/:upload_id/:index",
                put({
                    let chunk_attempts = Arc::clone(&chunk_attempts);
                    move || {
                        let chunk_attempts = Arc::clone(&chunk_attempts);
                        async move {
                            chunk_attempts.fetch_add(1, Ordering::SeqCst);
                            StatusCode::INTERNAL_SERVER_ERROR
                        }
                    }
                }),
            )
            .route(
                "/api/file/cancel/:upload_id",
                delete({
                    let cancel_seen = Arc::clone(&cancel_seen);
                    move || {
                        let cancel_seen = Arc::clone(&cancel_seen);
                        async move {
                            cancel_seen.fetch_add(1, Ordering::SeqCst);
                            StatusCode::OK
                        }
                    }
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let options = FileSendOptions {
            retry_attempts: 3,
            use_chunked: true,
            chunk_size: 64 * 1024,
            chunk_concurrency: 1,
            ..FileSendOptions::default()
        };
        let upload = tokio::spawn({
            let file_path = file_path.clone();
            async move {
                send_file_with_options(&server_addr, "client-sender", &file_path, options).await
            }
        });

        while chunk_attempts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        upload.abort();

        for _ in 0..100 {
            if cancel_seen.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(cancel_seen.load(Ordering::SeqCst), 1);
        assert_eq!(chunk_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_send_files_uploads_multiple_paths() {
        let registry = PeerRegistry::new();
        let download_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let router = make_router(registry, download_dir.path().to_path_buf());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let first = source_dir.path().join("first.txt");
        let second = source_dir.path().join("second.txt");
        tokio::fs::write(&first, "first").await.unwrap();
        tokio::fs::write(&second, "second").await.unwrap();

        send_files(
            &server_addr,
            "client-sender",
            &[first.clone(), second.clone()],
            2,
            FileSendOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            tokio::fs::read_to_string(download_dir.path().join("first.txt"))
                .await
                .unwrap(),
            "first"
        );
        assert_eq!(
            tokio::fs::read_to_string(download_dir.path().join("second.txt"))
                .await
                .unwrap(),
            "second"
        );
    }

    #[tokio::test]
    async fn test_send_files_aborts_in_flight_uploads_after_failure() {
        let source_dir = tempdir().unwrap();
        let slow_file = source_dir.path().join("slow.bin");
        let fail_file = source_dir.path().join("fail.bin");
        tokio::fs::write(&slow_file, vec![b's'; 1024])
            .await
            .unwrap();
        tokio::fs::write(&fail_file, vec![b'f'; 1024])
            .await
            .unwrap();

        let slow_chunk_started = Arc::new(AtomicUsize::new(0));
        let slow_chunk_started_notify = Arc::new(tokio::sync::Notify::new());
        let cancel_seen = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/file/init",
                post(|Json(payload): Json<serde_json::Value>| async move {
                    let file_name = payload
                        .get("file_name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let upload_id = if file_name == "slow.bin" {
                        "slow-upload"
                    } else {
                        "fail-upload"
                    };
                    let chunk_size = payload
                        .get("chunk_size")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(1024);
                    Json(json!({
                        "upload_id": upload_id,
                        "chunk_size": chunk_size,
                        "received_chunks": [],
                        "received_bytes": 0,
                    }))
                }),
            )
            .route(
                "/api/file/chunk/:upload_id/:index",
                put({
                    let slow_chunk_started = Arc::clone(&slow_chunk_started);
                    let slow_chunk_started_notify = Arc::clone(&slow_chunk_started_notify);
                    move |AxumPath((upload_id, _index)): AxumPath<(String, u64)>| {
                        let slow_chunk_started = Arc::clone(&slow_chunk_started);
                        let slow_chunk_started_notify = Arc::clone(&slow_chunk_started_notify);
                        async move {
                            if upload_id == "slow-upload" {
                                slow_chunk_started.store(1, Ordering::SeqCst);
                                slow_chunk_started_notify.notify_waiters();
                                std::future::pending::<StatusCode>().await
                            } else {
                                while slow_chunk_started.load(Ordering::SeqCst) == 0 {
                                    slow_chunk_started_notify.notified().await;
                                }
                                StatusCode::INTERNAL_SERVER_ERROR
                            }
                        }
                    }
                }),
            )
            .route(
                "/api/file/cancel/:upload_id",
                delete({
                    let cancel_seen = Arc::clone(&cancel_seen);
                    move |AxumPath(upload_id): AxumPath<String>| {
                        let cancel_seen = Arc::clone(&cancel_seen);
                        async move {
                            if upload_id == "slow-upload" {
                                cancel_seen.fetch_add(1, Ordering::SeqCst);
                            }
                            StatusCode::OK
                        }
                    }
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let options = FileSendOptions {
            use_chunked: true,
            chunk_size: 1024,
            chunk_concurrency: 1,
            ..FileSendOptions::default()
        };

        let result = tokio::time::timeout(
            Duration::from_millis(500),
            send_files(
                &server_addr,
                "client-sender",
                &[slow_file.clone(), fail_file.clone()],
                2,
                options,
            ),
        )
        .await;

        assert!(
            matches!(result, Ok(Err(_))),
            "send_files should return the first failure instead of waiting for another in-flight upload"
        );

        for _ in 0..20 {
            if cancel_seen.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(cancel_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_send_file_chunked_uploads_file() {
        let registry = PeerRegistry::new();
        let download_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let router = make_router(registry, download_dir.path().to_path_buf());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let file_path = source_dir.path().join("chunked.txt");
        let content = "chunked body ".repeat(2048);
        tokio::fs::write(&file_path, &content).await.unwrap();

        let options = FileSendOptions {
            use_chunked: true,
            chunk_size: 1024,
            chunk_concurrency: 3,
            ..FileSendOptions::default()
        };
        send_file_with_options(&server_addr, "client-sender", &file_path, options)
            .await
            .unwrap();

        let saved = tokio::fs::read_to_string(download_dir.path().join("chunked.txt"))
            .await
            .unwrap();
        assert_eq!(saved, content);
    }

    #[tokio::test]
    async fn test_send_empty_file_chunked() {
        let registry = PeerRegistry::new();
        let download_dir = tempdir().unwrap();
        let source_dir = tempdir().unwrap();
        let router = make_router(registry, download_dir.path().to_path_buf());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let file_path = source_dir.path().join("empty.txt");
        tokio::fs::write(&file_path, "").await.unwrap();

        let options = FileSendOptions {
            use_chunked: true,
            chunk_size: 1024,
            chunk_concurrency: 1,
            ..FileSendOptions::default()
        };
        send_file_with_options(&server_addr, "client-sender", &file_path, options)
            .await
            .unwrap();

        let saved = tokio::fs::read_to_string(download_dir.path().join("empty.txt"))
            .await
            .unwrap();
        assert_eq!(saved, "");
    }
}

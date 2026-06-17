use crate::client::{send_file_with_options, send_text, FileSendOptions, ProgressMode};
use crate::peer::Peer;
use crate::peer::PeerRegistry;
use crate::web_ui::{self, SseMessage, WebRuntimeInfo};
use axum::{
    body::Body,
    extract::{multipart::Field, ConnectInfo, DefaultBodyLimit, Multipart, Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use futures_util::stream;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::{Mutex, Notify};
use tokio_util::io::StreamReader;

/// Reject control-plane requests that do not originate from the local machine.
/// Receiving endpoints stay open to the LAN; config/runtime/web-send endpoints
/// must only be driven from localhost.
async fn require_localhost(
    connect_info: Option<ConnectInfo<SocketAddr>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match connect_info {
        Some(ConnectInfo(addr)) if addr.ip().is_loopback() => next.run(request).await,
        _ => StatusCode::FORBIDDEN.into_response(),
    }
}

type UploadSessions = Arc<Mutex<HashMap<String, UploadSessionEntry>>>;

#[derive(Clone)]
pub struct ServerState {
    pub registry: PeerRegistry,
    pub download_dir: PathBuf,
    upload_sessions: UploadSessions,
    web_info: Option<WebRuntimeInfo>,
    sse_tx: Option<BroadcastSender<String>>,
}

#[derive(Deserialize)]
pub struct MessagePayload {
    pub sender_name: String,
    pub text: String,
}

#[derive(Deserialize)]
struct WebMessagePayload {
    target: Option<String>,
    text: String,
}

#[derive(Serialize)]
pub struct StandardResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_bytes: Option<u64>,
}

#[derive(Serialize)]
struct WebSendResponse {
    status: String,
    delivered: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Clone)]
struct UploadSession {
    sender_name: String,
    final_path: PathBuf,
    temp_dir: PathBuf,
    file_size: u64,
    chunk_size: u64,
    checksum: String,
    received_chunks: BTreeSet<u64>,
    last_active: std::time::Instant,
}

#[derive(Clone)]
enum UploadSessionEntry {
    Active(UploadSession),
    Removing(Arc<Notify>),
}

#[derive(Serialize, Deserialize)]
struct InitUploadRequest {
    sender_name: String,
    file_name: String,
    file_size: u64,
    checksum: String,
    chunk_size: u64,
    upload_id: Option<String>,
}

#[derive(Serialize)]
struct InitUploadResponse {
    upload_id: String,
    chunk_size: u64,
    received_chunks: Vec<u64>,
    received_bytes: u64,
}

pub fn make_router(registry: PeerRegistry, download_dir: PathBuf) -> Router {
    make_router_with_events_and_info(registry, download_dir, None, None)
}

pub fn make_router_with_events_and_info(
    registry: PeerRegistry,
    download_dir: PathBuf,
    web_info: Option<WebRuntimeInfo>,
    sse_tx: Option<BroadcastSender<String>>,
) -> Router {
    let state = ServerState {
        registry,
        download_dir,
        upload_sessions: Arc::new(Mutex::new(HashMap::new())),
        web_info,
        sse_tx,
    };

    #[cfg(target_pointer_width = "64")]
    let max_file_limit = 10 * 1024 * 1024 * 1024;
    #[cfg(not(target_pointer_width = "64"))]
    let max_file_limit = 3 * 1024 * 1024 * 1024;

    let sessions = state.upload_sessions.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            cleanup_stale_upload_sessions(sessions.clone(), tokio::time::Duration::from_secs(3600))
                .await;
        }
    });

    let control_plane = Router::new()
        .route("/api/runtime", get(handle_runtime))
        .route(
            "/api/config",
            get(handle_get_config).post(handle_save_config),
        )
        .route(
            "/api/web/message",
            post(handle_web_message).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/web/file",
            post(handle_web_file).layer(DefaultBodyLimit::max(max_file_limit)),
        )
        .route_layer(axum::middleware::from_fn(require_localhost));

    Router::new()
        .route("/", get(web_ui::index))
        .route("/app", get(web_ui::index))
        .route("/api/peers", get(handle_peers))
        .route("/api/events", get(handle_sse_events))
        .merge(control_plane)
        .route(
            "/api/message",
            post(handle_message).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/file",
            post(handle_file).layer(DefaultBodyLimit::max(max_file_limit)),
        )
        .route(
            "/api/file/init",
            post(handle_file_init).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/file/chunk/:upload_id/:index",
            put(handle_file_chunk).layer(tower_http::limit::RequestBodyLimitLayer::new(
                32 * 1024 * 1024,
            )),
        )
        .route(
            "/api/file/complete/:upload_id",
            post(handle_file_complete).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .route(
            "/api/file/cancel/:upload_id",
            delete(handle_file_cancel).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        .with_state(state)
}

async fn handle_peers(State(state): State<ServerState>) -> Json<Vec<crate::peer::Peer>> {
    Json(state.registry.list())
}

async fn handle_runtime(State(state): State<ServerState>) -> impl IntoResponse {
    match state.web_info {
        Some(info) => Json(info).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn handle_sse_events(State(state): State<ServerState>) -> impl IntoResponse {
    if let Some(sse_tx) = &state.sse_tx {
        let rx = sse_tx.subscribe();
        web_ui::sse_events(rx).await.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

#[derive(Deserialize)]
struct ConfigSavePayload {
    defaults: Option<crate::config::ConfigDefaults>,
}

#[derive(Serialize)]
struct ConfigResponse {
    defaults: crate::config::ConfigDefaults,
}

async fn handle_get_config() -> impl IntoResponse {
    let config = crate::config::AppConfig::load().unwrap_or_default();
    Json(ConfigResponse {
        defaults: config.defaults,
    })
}

async fn handle_save_config(
    payload: Result<Json<ConfigSavePayload>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(p) => p,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let defaults = payload.defaults.unwrap_or_default();
    let config = crate::config::AppConfig { defaults };
    let config_path = crate::config::config_file_path();
    if let Some(path) = &config_path {
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"message": format!("无法创建配置目录: {}", e)})),
                )
                    .into_response();
            }
        }
        let content = toml::to_string_pretty(&config).unwrap_or_default();
        if let Err(e) = std::fs::write(path, content) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"message": format!("无法写入配置文件: {}", e)})),
            )
                .into_response();
        }
        Json(ConfigResponse {
            defaults: config.defaults,
        })
        .into_response()
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"message": "无法确定配置文件路径"})),
        )
            .into_response()
    }
}

struct StaleUploadCleanup {
    upload_id: String,
    session: UploadSession,
    notify: Arc<Notify>,
}

async fn cleanup_stale_upload_sessions(sessions: UploadSessions, timeout: std::time::Duration) {
    cleanup_stale_upload_sessions_with(sessions, timeout, remove_upload_session_files).await;
}

async fn cleanup_stale_upload_sessions_with<F, Fut>(
    sessions: UploadSessions,
    timeout: std::time::Duration,
    cleanup: F,
) where
    F: Fn(UploadSession) -> Fut,
    Fut: Future<Output = ()>,
{
    let stale_sessions = take_stale_upload_sessions(&sessions, timeout).await;

    for stale in stale_sessions {
        cleanup(stale.session).await;
        finish_stale_upload_cleanup(&sessions, &stale.upload_id, &stale.notify).await;
    }
}

async fn take_stale_upload_sessions(
    sessions: &UploadSessions,
    timeout: std::time::Duration,
) -> Vec<StaleUploadCleanup> {
    let mut map = sessions.lock().await;
    let now = std::time::Instant::now();
    let stale_ids: Vec<String> = map
        .iter()
        .filter_map(|(id, entry)| match entry {
            UploadSessionEntry::Active(session)
                if now.duration_since(session.last_active) > timeout =>
            {
                Some(id.clone())
            }
            _ => None,
        })
        .collect();

    let mut stale_sessions = Vec::with_capacity(stale_ids.len());
    for id in stale_ids {
        let Some(UploadSessionEntry::Active(session)) = map.remove(&id) else {
            continue;
        };
        let notify = Arc::new(Notify::new());
        map.insert(id.clone(), UploadSessionEntry::Removing(notify.clone()));
        stale_sessions.push(StaleUploadCleanup {
            upload_id: id,
            session,
            notify,
        });
    }

    stale_sessions
}

async fn finish_stale_upload_cleanup(
    sessions: &UploadSessions,
    upload_id: &str,
    notify: &Arc<Notify>,
) {
    let mut map = sessions.lock().await;
    if matches!(
        map.get(upload_id),
        Some(UploadSessionEntry::Removing(existing)) if Arc::ptr_eq(existing, notify)
    ) {
        map.remove(upload_id);
    }
    drop(map);
    notify.notify_waiters();
}

async fn remove_upload_session_files(session: UploadSession) {
    let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;
    if tokio::fs::try_exists(&session.final_path)
        .await
        .unwrap_or(false)
    {
        if let Ok(metadata) = tokio::fs::metadata(&session.final_path).await {
            if metadata.len() == 0 {
                let _ = tokio::fs::remove_file(&session.final_path).await;
            }
        }
    }
}

async fn handle_web_message(
    State(state): State<ServerState>,
    payload: Result<Json<WebMessagePayload>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => return web_send_error(StatusCode::BAD_REQUEST, "消息格式无效"),
    };
    let text = payload.text.trim();
    if text.is_empty() {
        return web_send_error(StatusCode::BAD_REQUEST, "消息内容不能为空");
    }

    let targets = match resolve_web_targets(&state, payload.target.as_deref()) {
        Ok(targets) => targets,
        Err(message) => return web_send_error(StatusCode::NOT_FOUND, message),
    };
    let sender = web_sender_name(&state);
    let mut delivered = 0usize;
    let mut last_error = None;

    for peer in targets {
        let Some(addr) = peer_address(&peer).await else {
            last_error = Some(format!("节点 '{}' 没有可用地址", peer.name));
            continue;
        };
        match send_text(&addr, &sender, text).await {
            Ok(()) => delivered += 1,
            Err(err) => last_error = Some(format!("发送到 '{}' 失败: {}", peer.name, err)),
        }
    }

    if delivered == 0 {
        return web_send_error(
            StatusCode::BAD_GATEWAY,
            last_error.as_deref().unwrap_or("没有节点接收成功"),
        );
    }

    (
        StatusCode::OK,
        Json(WebSendResponse {
            status: "success".to_string(),
            delivered,
            message: last_error,
        }),
    )
        .into_response()
}

async fn handle_web_file(
    State(state): State<ServerState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut target = None;
    let mut saved_file = None;

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(_) => return web_send_error(StatusCode::BAD_REQUEST, "文件上传格式无效"),
        };
        match field.name() {
            Some("target") => {
                if let Ok(value) = field.text().await {
                    let value = value.trim().to_string();
                    if !value.is_empty() {
                        target = Some(value);
                    }
                }
            }
            Some("file") => match save_web_file_field(field).await {
                Ok(path) => saved_file = Some(path),
                Err(message) => return web_send_error(StatusCode::BAD_REQUEST, &message),
            },
            _ => {}
        }
    }

    let Some(file_path) = saved_file else {
        return web_send_error(StatusCode::BAD_REQUEST, "缺少要发送的文件");
    };

    let targets = match resolve_web_targets(&state, target.as_deref()) {
        Ok(targets) => targets,
        Err(message) => {
            let _ = tokio::fs::remove_file(&file_path).await;
            return web_send_error(StatusCode::NOT_FOUND, message);
        }
    };
    let sender = web_sender_name(&state);
    let options = FileSendOptions {
        progress: ProgressMode::None,
        ..FileSendOptions::default()
    };
    let mut delivered = 0usize;
    let mut last_error = None;

    for peer in targets {
        let Some(addr) = peer_address(&peer).await else {
            last_error = Some(format!("节点 '{}' 没有可用地址", peer.name));
            continue;
        };
        match send_file_with_options(&addr, &sender, &file_path, options.clone()).await {
            Ok(()) => delivered += 1,
            Err(err) => last_error = Some(format!("发送到 '{}' 失败: {}", peer.name, err)),
        }
    }

    let _ = tokio::fs::remove_file(&file_path).await;

    if delivered == 0 {
        return web_send_error(
            StatusCode::BAD_GATEWAY,
            last_error.as_deref().unwrap_or("没有节点接收成功"),
        );
    }

    (
        StatusCode::OK,
        Json(WebSendResponse {
            status: "success".to_string(),
            delivered,
            message: last_error,
        }),
    )
        .into_response()
}

async fn save_web_file_field(mut field: Field<'_>) -> Result<PathBuf, String> {
    let file_name = field
        .file_name()
        .and_then(|name| Path::new(name).file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "文件名无效".to_string())?
        .to_string();
    let temp_path = std::env::temp_dir().join(format!(
        "lan-share-web-{}-{}",
        uuid::Uuid::new_v4(),
        file_name
    ));
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(|err| format!("无法创建临时文件: {}", err))?;

    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|err| format!("读取上传文件失败: {}", err))?
    {
        file.write_all(&chunk)
            .await
            .map_err(|err| format!("写入临时文件失败: {}", err))?;
    }

    Ok(temp_path)
}

fn resolve_web_targets(
    state: &ServerState,
    target: Option<&str>,
) -> Result<Vec<Peer>, &'static str> {
    if let Some(target) = target.map(str::trim).filter(|target| !target.is_empty()) {
        return state
            .registry
            .find_by_name_or_ip(target)
            .map(|peer| vec![peer])
            .ok_or("未找到指定节点");
    }

    let peers = state.registry.list();
    if peers.is_empty() {
        Err("当前没有可发送的在线节点")
    } else {
        Ok(peers)
    }
}

fn web_sender_name(state: &ServerState) -> String {
    state
        .web_info
        .as_ref()
        .map(|info| info.node_name.clone())
        .unwrap_or_else(|| "Web".to_string())
}

async fn peer_address(peer: &Peer) -> Option<String> {
    let candidates: Vec<String> = peer
        .ips
        .iter()
        .map(|ip| format!("{}:{}", ip, peer.port))
        .collect();
    if candidates.is_empty() {
        return None;
    }
    crate::client::pick_reachable_address(&candidates, std::time::Duration::from_millis(1500)).await
}

fn web_send_error(status: StatusCode, message: &str) -> axum::response::Response {
    (
        status,
        Json(WebSendResponse {
            status: "error".to_string(),
            delivered: 0,
            message: Some(message.to_string()),
        }),
    )
        .into_response()
}

async fn handle_message(
    State(state): State<ServerState>,
    payload: Result<Json<MessagePayload>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(p) => p,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    println!(
        "\n[收到来自 {} 的文字消息]: {}",
        payload.sender_name, payload.text
    );
    // Broadcast SSE event for web clients
    if let Some(sse_tx) = &state.sse_tx {
        let msg = SseMessage::message(payload.sender_name.clone(), payload.text.clone());
        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = sse_tx.send(json);
        }
    }
    Json(StandardResponse {
        status: "success".to_string(),
        received_bytes: None,
    })
    .into_response()
}

async fn handle_file(
    State(state): State<ServerState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if let Err(e) = tokio::fs::create_dir_all(&state.download_dir).await {
        eprintln!("Failed to create download directory: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let mut sender_name = String::new();
    let mut checksum_mode = None;
    let mut checksum = None;
    let mut file_encoding = "none".to_string();
    let mut saved_file_path = None;
    let mut received_bytes = 0;

    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Multipart error while reading field: {}", e);
                return StatusCode::BAD_REQUEST.into_response();
            }
        };

        let name = match field.name() {
            Some(n) => n.to_string(),
            None => continue,
        };

        if name == "sender_name" {
            if let Ok(text) = field.text().await {
                sender_name = text;
            }
        } else if name == "checksum_mode" {
            if let Ok(text) = field.text().await {
                checksum_mode = Some(text);
            }
        } else if name == "checksum" {
            if let Ok(text) = field.text().await {
                checksum = Some(text);
            }
        } else if name == "file_encoding" {
            if let Ok(text) = field.text().await {
                file_encoding = text;
            }
        } else if name == "file" {
            let fname = match field.file_name() {
                Some(fname) => fname,
                None => return StatusCode::BAD_REQUEST.into_response(),
            };

            // 防范路径穿越安全漏洞
            let basename = match std::path::Path::new(fname)
                .file_name()
                .and_then(|f| f.to_str())
            {
                Some(b) if !b.is_empty() => b,
                _ => return StatusCode::BAD_REQUEST.into_response(),
            };

            if checksum_mode.as_deref() != Some("sha256") {
                eprintln!("Missing or unsupported checksum mode");
                return StatusCode::BAD_REQUEST.into_response();
            }
            let checksum = match checksum.as_deref().filter(|value| is_sha256_hex(value)) {
                Some(value) => value.to_string(),
                None => {
                    eprintln!("Missing or invalid checksum");
                    return StatusCode::BAD_REQUEST.into_response();
                }
            };

            let final_path = match reserve_download_path(&state, basename).await {
                Ok(path) => path,
                Err(status) => return status.into_response(),
            };
            let temp_path = temp_upload_path(&state.download_dir, basename);
            match save_upload_field(field, &temp_path, &final_path, &checksum, &file_encoding).await
            {
                Ok(bytes) => {
                    received_bytes = bytes;
                    saved_file_path = Some(final_path);
                }
                Err(UploadError::BadRequest(message)) => {
                    eprintln!("{}", message);
                    let _ = tokio::fs::remove_file(&final_path).await;
                    return StatusCode::BAD_REQUEST.into_response();
                }
                Err(UploadError::Io(e)) => {
                    eprintln!("Failed to save uploaded file: {}", e);
                    let _ = tokio::fs::remove_file(&final_path).await;
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                Err(UploadError::ClientDisconnected) => {
                    let _ = tokio::fs::remove_file(&final_path).await;
                    return client_closed_request().into_response();
                }
            }
        }
    }

    let final_path = match saved_file_path {
        Some(path) => path,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let file_name = final_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    println!(
        "\n[成功接收文件] 来自: {}, 保存至: {}",
        sender_name,
        final_path.display()
    );

    // Broadcast SSE event for web clients
    if let Some(sse_tx) = &state.sse_tx {
        let msg = SseMessage::file(sender_name.clone(), file_name.clone(), received_bytes);
        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = sse_tx.send(json);
        }
    }

    Json(StandardResponse {
        status: "success".to_string(),
        received_bytes: Some(received_bytes),
    })
    .into_response()
}

async fn handle_file_init(
    State(state): State<ServerState>,
    payload: Result<Json<InitUploadRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    if let Err(e) = tokio::fs::create_dir_all(&state.download_dir).await {
        eprintln!("Failed to create download directory: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    if payload.chunk_size == 0 || !is_sha256_hex(&payload.checksum) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let basename = match Path::new(&payload.file_name)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };

    let upload_id = payload
        .upload_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    loop {
        let wait_for_cleanup = {
            let mut sessions = state.upload_sessions.lock().await;
            match sessions.get_mut(&upload_id) {
                Some(UploadSessionEntry::Active(session)) => {
                    if session.file_size != payload.file_size
                        || session.chunk_size != payload.chunk_size
                        || session.checksum != payload.checksum
                    {
                        return StatusCode::BAD_REQUEST.into_response();
                    }
                    session.last_active = std::time::Instant::now();
                    return Json(init_response(&upload_id, session)).into_response();
                }
                Some(UploadSessionEntry::Removing(notify)) => Some(notify.clone()),
                None => None,
            }
        };
        if let Some(notify) = wait_for_cleanup {
            notify.notified().await;
            continue;
        }

        let final_path = match reserve_download_path(&state, &basename).await {
            Ok(path) => path,
            Err(status) => return status.into_response(),
        };
        let temp_dir = chunk_upload_dir(&state.download_dir, &basename, &upload_id);
        if let Err(e) = tokio::fs::create_dir_all(&temp_dir).await {
            eprintln!("Failed to create upload temp directory: {}", e);
            let _ = tokio::fs::remove_file(&final_path).await;
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        let received_chunks =
            match scan_received_chunks(&temp_dir, payload.file_size, payload.chunk_size).await {
                Ok(chunks) => chunks,
                Err(e) => {
                    eprintln!("Failed to scan upload temp directory: {}", e);
                    let _ = tokio::fs::remove_file(&final_path).await;
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

        let mut placeholder_to_remove = None;
        let mut sessions = state.upload_sessions.lock().await;
        let session_result = match sessions.get_mut(&upload_id) {
            Some(UploadSessionEntry::Active(existing)) => {
                placeholder_to_remove = Some(final_path);
                if existing.file_size != payload.file_size
                    || existing.chunk_size != payload.chunk_size
                    || existing.checksum != payload.checksum
                {
                    Err(StatusCode::BAD_REQUEST)
                } else {
                    existing.last_active = std::time::Instant::now();
                    Ok(existing.clone())
                }
            }
            Some(UploadSessionEntry::Removing(notify)) => {
                let notify = notify.clone();
                drop(sessions);
                let _ = tokio::fs::remove_file(&final_path).await;
                notify.notified().await;
                continue;
            }
            None => {
                let new_session = UploadSession {
                    sender_name: payload.sender_name.clone(),
                    final_path,
                    temp_dir,
                    file_size: payload.file_size,
                    chunk_size: payload.chunk_size,
                    checksum: payload.checksum.clone(),
                    received_chunks,
                    last_active: std::time::Instant::now(),
                };
                sessions.insert(
                    upload_id.clone(),
                    UploadSessionEntry::Active(new_session.clone()),
                );
                Ok(new_session)
            }
        };
        drop(sessions);

        if let Some(path) = placeholder_to_remove {
            let _ = tokio::fs::remove_file(path).await;
        }

        let session = match session_result {
            Ok(session) => session,
            Err(status) => return status.into_response(),
        };

        let response = init_response(&upload_id, &session);
        return Json(response).into_response();
    }
}

async fn handle_file_chunk(
    State(state): State<ServerState>,
    AxumPath((upload_id, index)): AxumPath<(String, u64)>,
    body: Body,
) -> impl IntoResponse {
    let session = match state.upload_sessions.lock().await.get(&upload_id).cloned() {
        Some(UploadSessionEntry::Active(session)) => session,
        None => return StatusCode::NOT_FOUND.into_response(),
        Some(UploadSessionEntry::Removing(_)) => return StatusCode::NOT_FOUND.into_response(),
    };

    let expected_chunks = chunk_count(session.file_size, session.chunk_size);
    if index >= expected_chunks {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let expected_bytes = chunk_len(session.file_size, session.chunk_size, index);
    let chunk_path = chunk_path(&session.temp_dir, index);
    if chunk_path.exists() {
        if let Some(UploadSessionEntry::Active(session)) =
            state.upload_sessions.lock().await.get_mut(&upload_id)
        {
            session.received_chunks.insert(index);
            session.last_active = std::time::Instant::now();
        }
        return Json(StandardResponse {
            status: "success".to_string(),
            received_bytes: Some(expected_bytes),
        })
        .into_response();
    }

    match save_chunk(body, &session.temp_dir, index, expected_bytes).await {
        Ok(written) => {
            if let Some(UploadSessionEntry::Active(session)) =
                state.upload_sessions.lock().await.get_mut(&upload_id)
            {
                session.received_chunks.insert(index);
                session.last_active = std::time::Instant::now();
            }
            Json(StandardResponse {
                status: "success".to_string(),
                received_bytes: Some(written),
            })
            .into_response()
        }
        Err(UploadError::BadRequest(message)) => {
            eprintln!("{}", message);
            StatusCode::BAD_REQUEST.into_response()
        }
        Err(UploadError::Io(e)) => {
            if e.kind() == io::ErrorKind::NotFound
                && !tokio::fs::try_exists(&session.temp_dir)
                    .await
                    .unwrap_or(false)
            {
                return StatusCode::NOT_FOUND.into_response();
            }
            eprintln!("Failed to save chunk: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(UploadError::ClientDisconnected) => client_closed_request().into_response(),
    }
}

async fn handle_file_complete(
    State(state): State<ServerState>,
    AxumPath(upload_id): AxumPath<String>,
) -> impl IntoResponse {
    let session = match state.upload_sessions.lock().await.get(&upload_id).cloned() {
        Some(UploadSessionEntry::Active(session)) => session,
        None => return StatusCode::NOT_FOUND.into_response(),
        Some(UploadSessionEntry::Removing(_)) => return StatusCode::NOT_FOUND.into_response(),
    };

    let expected_chunks = chunk_count(session.file_size, session.chunk_size);
    if session.received_chunks.len() != expected_chunks as usize {
        return StatusCode::BAD_REQUEST.into_response();
    }

    match complete_chunked_upload(&session).await {
        Ok(()) => {
            let mut sessions = state.upload_sessions.lock().await;
            if matches!(
                sessions.get(&upload_id),
                Some(UploadSessionEntry::Active(_))
            ) {
                sessions.remove(&upload_id);
            }
            drop(sessions);
            let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;

            let file_name = session
                .final_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            println!(
                "\n[成功接收分片文件] 来自: {}, 保存至: {}",
                session.sender_name,
                session.final_path.display()
            );

            // Broadcast SSE event for web clients
            if let Some(sse_tx) = &state.sse_tx {
                let msg = SseMessage::file(
                    session.sender_name.clone(),
                    file_name.clone(),
                    session.file_size,
                );
                if let Ok(json) = serde_json::to_string(&msg) {
                    let _ = sse_tx.send(json);
                }
            }
            Json(StandardResponse {
                status: "success".to_string(),
                received_bytes: Some(session.file_size),
            })
            .into_response()
        }
        Err(UploadError::BadRequest(message)) => {
            eprintln!("{}", message);
            StatusCode::BAD_REQUEST.into_response()
        }
        Err(UploadError::Io(e)) => {
            eprintln!("Failed to complete chunked upload: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(UploadError::ClientDisconnected) => client_closed_request().into_response(),
    }
}

async fn handle_file_cancel(
    State(state): State<ServerState>,
    AxumPath(upload_id): AxumPath<String>,
) -> impl IntoResponse {
    let session = {
        let mut sessions = state.upload_sessions.lock().await;
        if matches!(
            sessions.get(&upload_id),
            Some(UploadSessionEntry::Active(_))
        ) {
            match sessions.remove(&upload_id) {
                Some(UploadSessionEntry::Active(session)) => Some(session),
                _ => None,
            }
        } else {
            None
        }
    };
    let Some(session) = session else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;
    let _ = tokio::fs::remove_file(&session.final_path).await;

    Json(StandardResponse {
        status: "canceled".to_string(),
        received_bytes: Some(received_bytes_for_chunks(&session)),
    })
    .into_response()
}

enum UploadError {
    BadRequest(String),
    ClientDisconnected,
    Io(io::Error),
}

impl From<io::Error> for UploadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

async fn reserve_download_path(state: &ServerState, basename: &str) -> Result<PathBuf, StatusCode> {
    let original_path = Path::new(basename);
    let stem = original_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let extension = original_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let mut counter = 0;
    loop {
        let candidate = if counter == 0 {
            state.download_dir.join(basename)
        } else if extension.is_empty() {
            state.download_dir.join(format!("{}_{}", stem, counter))
        } else {
            state
                .download_dir
                .join(format!("{}_{}.{}", stem, counter, extension))
        };
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(_) => return Ok(candidate),
            Err(ref e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                counter += 1;
            }
            Err(e) => {
                eprintln!("Failed to create placeholder file: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }
}

fn client_closed_request() -> StatusCode {
    StatusCode::from_u16(499).expect("499 is a valid non-standard HTTP status code")
}

fn temp_upload_path(download_dir: &Path, basename: &str) -> PathBuf {
    download_dir.join(format!(".{}.part-{}", basename, uuid::Uuid::new_v4()))
}

fn chunk_upload_dir(download_dir: &Path, basename: &str, upload_id: &str) -> PathBuf {
    download_dir.join(format!(".{}.chunks-{}", basename, upload_id))
}

fn chunk_path(temp_dir: &Path, index: u64) -> PathBuf {
    temp_dir.join(format!("chunk_{}", index))
}

fn chunk_count(file_size: u64, chunk_size: u64) -> u64 {
    file_size.div_ceil(chunk_size.max(1))
}

fn chunk_len(file_size: u64, chunk_size: u64, index: u64) -> u64 {
    if file_size == 0 {
        0
    } else {
        let offset = index * chunk_size;
        (file_size - offset).min(chunk_size)
    }
}

fn init_response(upload_id: &str, session: &UploadSession) -> InitUploadResponse {
    InitUploadResponse {
        upload_id: upload_id.to_string(),
        chunk_size: session.chunk_size,
        received_chunks: session.received_chunks.iter().copied().collect(),
        received_bytes: received_bytes_for_chunks(session),
    }
}

fn received_bytes_for_chunks(session: &UploadSession) -> u64 {
    session
        .received_chunks
        .iter()
        .map(|index| chunk_len(session.file_size, session.chunk_size, *index))
        .sum()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn scan_received_chunks(
    temp_dir: &Path,
    file_size: u64,
    chunk_size: u64,
) -> io::Result<BTreeSet<u64>> {
    let mut chunks = BTreeSet::new();
    let mut entries = tokio::fs::read_dir(temp_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(index) = file_name
            .strip_prefix("chunk_")
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if index >= chunk_count(file_size, chunk_size) {
            continue;
        }
        let expected = chunk_len(file_size, chunk_size, index);
        if entry.metadata().await?.len() == expected {
            chunks.insert(index);
        }
    }
    Ok(chunks)
}

async fn save_chunk(
    mut body: Body,
    temp_dir: &Path,
    index: u64,
    expected_bytes: u64,
) -> Result<u64, UploadError> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let temp_path = temp_dir.join(format!("chunk_{}_{}.part", index, request_id));
    let final_path = chunk_path(temp_dir, index);

    struct TempFileGuard {
        path: std::path::PathBuf,
        completed: bool,
    }
    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            if !self.completed {
                let path = self.path.clone();
                tokio::spawn(async move {
                    let _ = tokio::fs::remove_file(path).await;
                });
            }
        }
    }

    let mut guard = TempFileGuard {
        path: temp_path.clone(),
        completed: false,
    };

    let result = save_chunk_inner(&mut body, &temp_path, &final_path, expected_bytes).await;
    if result.is_ok() {
        guard.completed = true;
    }
    result
}

async fn save_chunk_inner(
    body: &mut Body,
    temp_path: &Path,
    final_path: &Path,
    expected_bytes: u64,
) -> Result<u64, UploadError> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp_path)
        .await?;
    let mut written = 0;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| UploadError::ClientDisconnected)?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        written += data.len() as u64;
        if written > expected_bytes {
            return Err(UploadError::BadRequest(format!(
                "Chunk {} is larger than expected",
                final_path.display()
            )));
        }
        output.write_all(data).await?;
    }

    if written != expected_bytes {
        return Err(UploadError::BadRequest(format!(
            "Chunk {} size mismatch: expected {}, got {}",
            final_path.display(),
            expected_bytes,
            written
        )));
    }

    drop(output);
    tokio::fs::rename(temp_path, final_path).await?;
    Ok(written)
}

async fn complete_chunked_upload(session: &UploadSession) -> Result<(), UploadError> {
    let basename = session
        .final_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload");
    let merge_path = temp_upload_path(
        session
            .final_path
            .parent()
            .unwrap_or_else(|| Path::new(".")),
        basename,
    );
    let result = complete_chunked_upload_inner(session, &merge_path).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&merge_path).await;
    }
    result
}

async fn complete_chunked_upload_inner(
    session: &UploadSession,
    merge_path: &Path,
) -> Result<(), UploadError> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(merge_path)
        .await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024]; // 1MB buffer

    for index in 0..chunk_count(session.file_size, session.chunk_size) {
        let mut chunk = tokio::fs::File::open(chunk_path(&session.temp_dir, index)).await?;
        loop {
            let read = chunk.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read]).await?;
            hasher.update(&buffer[..read]);
        }
    }

    output.sync_all().await?;
    drop(output);

    let actual_checksum = format!("{:x}", hasher.finalize());
    if actual_checksum != session.checksum {
        return Err(UploadError::BadRequest(format!(
            "Checksum mismatch: expected {}, got {}",
            session.checksum, actual_checksum
        )));
    }

    tokio::fs::rename(merge_path, &session.final_path).await?;
    Ok(())
}

async fn save_upload_field(
    field: Field<'_>,
    temp_path: &Path,
    final_path: &Path,
    expected_checksum: &str,
    file_encoding: &str,
) -> Result<u64, UploadError> {
    let result = save_upload_field_inner(
        field,
        temp_path,
        final_path,
        expected_checksum,
        file_encoding,
    )
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(temp_path).await;
    }
    result
}

async fn save_upload_field_inner(
    field: Field<'_>,
    temp_path: &Path,
    final_path: &Path,
    expected_checksum: &str,
    file_encoding: &str,
) -> Result<u64, UploadError> {
    let mut output = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
        .await?;

    let stream = stream::try_unfold(field, |mut field| async move {
        match field
            .chunk()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
        {
            Some(chunk) => Ok::<_, io::Error>(Some((chunk, field))),
            None => Ok::<_, io::Error>(None),
        }
    });
    let stream_reader = StreamReader::new(stream);
    let mut reader: Pin<Box<dyn AsyncRead + Send + '_>> = match file_encoding {
        "none" => Box::pin(stream_reader),
        "zstd" => Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(
            stream_reader,
        )),
        other => {
            return Err(UploadError::BadRequest(format!(
                "Unsupported file encoding: {}",
                other
            )));
        }
    };

    let mut hasher = Sha256::new();
    let mut received_bytes = 0;
    let mut buffer = vec![0; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read]).await?;
        hasher.update(&buffer[..read]);
        received_bytes += read as u64;
    }

    output.sync_all().await?;
    drop(output);

    let actual_checksum = format!("{:x}", hasher.finalize());
    if actual_checksum != expected_checksum {
        return Err(UploadError::BadRequest(format!(
            "Checksum mismatch: expected {}, got {}",
            expected_checksum, actual_checksum
        )));
    }

    tokio::fs::rename(temp_path, final_path).await?;
    Ok(received_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerRegistry;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use bytes::Bytes;
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn sha256_hex(content: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content);
        format!("{:x}", hasher.finalize())
    }

    fn control_plane_request(method: &str, uri: &str, remote: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if method == "POST" {
            builder = builder.header("content-type", "application/json");
        }
        let mut request = builder.body(Body::empty()).unwrap();
        if let Some(remote) = remote {
            let addr: SocketAddr = remote.parse().unwrap();
            request.extensions_mut().insert(ConnectInfo(addr));
        }
        request
    }

    #[tokio::test]
    async fn test_control_plane_allows_localhost() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = control_plane_request("GET", "/api/config", Some("127.0.0.1:54321"));
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_control_plane_rejects_remote_address() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = control_plane_request("GET", "/api/config", Some("192.168.1.50:54321"));
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_control_plane_rejects_missing_connect_info() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = control_plane_request("POST", "/api/config", None);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_receiving_endpoint_stays_open_without_connect_info() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = Request::builder()
            .method("POST")
            .uri("/api/message")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sender_name": "remote", "text": "hi"}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_receive_message() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = Request::builder()
            .method("POST")
            .uri("/api/message")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sender_name": "test-sender", "text": "Hello, world!"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "success");
    }

    #[tokio::test]
    async fn test_receive_message_bad_request() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let request = Request::builder()
            .method("POST")
            .uri("/api/message")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sender_name": "test-sender"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_cancel_chunked_upload_removes_partial_session_files() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());
        let upload_id = "cancel-me";
        let checksum = sha256_hex(b"partial file");

        let init_body = serde_json::json!({
            "sender_name": "test-sender",
            "file_name": "test.bin",
            "file_size": 12,
            "checksum": checksum,
            "chunk_size": 6,
            "upload_id": upload_id,
        });
        let request = Request::builder()
            .method("POST")
            .uri("/api/file/init")
            .header("content-type", "application/json")
            .body(Body::from(init_body.to_string()))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let temp_dir = tmp_dir.path().join(format!(".test.bin.chunks-{upload_id}"));
        tokio::fs::write(temp_dir.join("chunk_0"), b"partia")
            .await
            .unwrap();
        assert!(temp_dir.exists());
        assert!(tmp_dir.path().join("test.bin").exists());

        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/api/file/cancel/{upload_id}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!temp_dir.exists());
        assert!(!tmp_dir.path().join("test.bin").exists());
    }

    #[tokio::test]
    async fn test_missing_chunk_temp_directory_returns_not_found() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());
        let upload_id = "missing-temp-dir";
        let checksum = sha256_hex(b"partial file");

        let init_body = serde_json::json!({
            "sender_name": "test-sender",
            "file_name": "test.bin",
            "file_size": 12,
            "checksum": checksum,
            "chunk_size": 12,
            "upload_id": upload_id,
        });
        let request = Request::builder()
            .method("POST")
            .uri("/api/file/init")
            .header("content-type", "application/json")
            .body(Body::from(init_body.to_string()))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let temp_dir = tmp_dir.path().join(format!(".test.bin.chunks-{upload_id}"));
        tokio::fs::remove_dir_all(&temp_dir).await.unwrap();

        let request = Request::builder()
            .method("PUT")
            .uri(format!("/api/file/chunk/{upload_id}/0"))
            .body(Body::from("partial file"))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_chunk_body_disconnect_returns_client_closed_request() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());
        let upload_id = "body-disconnect";
        let checksum = sha256_hex(b"partial file");

        let init_body = serde_json::json!({
            "sender_name": "test-sender",
            "file_name": "test.bin",
            "file_size": 12,
            "checksum": checksum,
            "chunk_size": 12,
            "upload_id": upload_id,
        });
        let request = Request::builder()
            .method("POST")
            .uri("/api/file/init")
            .header("content-type", "application/json")
            .body(Body::from(init_body.to_string()))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let disconnecting_body = Body::from_stream(stream::once(async {
            Err::<Bytes, io::Error>(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "error reading a body from connection",
            ))
        }));
        let request = Request::builder()
            .method("PUT")
            .uri(format!("/api/file/chunk/{upload_id}/0"))
            .body(disconnecting_body)
            .unwrap();
        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::from_u16(499).unwrap());
    }

    #[tokio::test]
    async fn test_upload_file_success() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let boundary = "X-MULTIPART-BOUNDARY";
        let checksum = sha256_hex(b"Hello, file!");
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"sender_name\"\r\n\r\n\
             test-sender\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum_mode\"\r\n\r\n\
             sha256\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum\"\r\n\r\n\
             {checksum}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file_encoding\"\r\n\r\n\
             none\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             Hello, file!\r\n\
             --{boundary}--\r\n"
        );

        let request = Request::builder()
            .method("POST")
            .uri("/api/file")
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["received_bytes"], 12);

        // 检查文件是否成功保存
        let saved_path = tmp_dir.path().join("test.txt");
        assert!(saved_path.exists());
        let content = std::fs::read_to_string(saved_path).unwrap();
        assert_eq!(content, "Hello, file!");
        let temp_files: Vec<_> = std::fs::read_dir(tmp_dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".part-"))
            .collect();
        assert!(
            temp_files.is_empty(),
            "temporary upload files should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_upload_file_checksum_mismatch_removes_temp_file() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let boundary = "X-MULTIPART-BOUNDARY";
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"sender_name\"\r\n\r\n\
             test-sender\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum_mode\"\r\n\r\n\
             sha256\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum\"\r\n\r\n\
             0000000000000000000000000000000000000000000000000000000000000000\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file_encoding\"\r\n\r\n\
             none\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"bad.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             Corrupted content\r\n\
             --{boundary}--\r\n"
        );

        let request = Request::builder()
            .method("POST")
            .uri("/api/file")
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!tmp_dir.path().join("bad.txt").exists());
        let leftover_files: Vec<_> = std::fs::read_dir(tmp_dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(
            leftover_files.is_empty(),
            "failed uploads must not leave partial files"
        );
    }

    #[tokio::test]
    async fn test_upload_file_conflict() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();

        // 事先在下载目录创建 test.txt 和 test_1.txt
        let download_dir = tmp_dir.path();
        std::fs::write(download_dir.join("test.txt"), "old content").unwrap();
        std::fs::write(download_dir.join("test_1.txt"), "old content 1").unwrap();

        let router = make_router(registry, download_dir.to_path_buf());

        let boundary = "X-MULTIPART-BOUNDARY";
        let checksum = sha256_hex(b"New multipart data");
        let body = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"sender_name\"\r\n\r\n\
             test-sender\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum_mode\"\r\n\r\n\
             sha256\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"checksum\"\r\n\r\n\
             {checksum}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file_encoding\"\r\n\r\n\
             none\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             New multipart data\r\n\
             --{boundary}--\r\n"
        );

        let request = Request::builder()
            .method("POST")
            .uri("/api/file")
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "success");

        // 应该保存为 test_2.txt，因为 test.txt 和 test_1.txt 都存在
        let expected_path = download_dir.join("test_2.txt");
        assert!(expected_path.exists());
        let content = std::fs::read_to_string(expected_path).unwrap();
        assert_eq!(content, "New multipart data");
    }

    #[tokio::test]
    async fn test_chunked_upload_resume_reports_existing_chunks_and_completes() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let content = b"abcdef";
        let checksum = sha256_hex(content);
        let init_body = serde_json::json!({
            "sender_name": "chunk-sender",
            "file_name": "resume.txt",
            "file_size": content.len(),
            "checksum": checksum,
            "chunk_size": 3,
            "upload_id": "resume-test",
        });

        let init_request = Request::builder()
            .method("POST")
            .uri("/api/file/init")
            .header("content-type", "application/json")
            .body(Body::from(init_body.to_string()))
            .unwrap();
        let init_response = router.clone().oneshot(init_request).await.unwrap();
        assert_eq!(init_response.status(), StatusCode::OK);

        let first_chunk = Request::builder()
            .method("PUT")
            .uri("/api/file/chunk/resume-test/0")
            .body(Body::from("abc"))
            .unwrap();
        let first_response = router.clone().oneshot(first_chunk).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);

        let resume_request = Request::builder()
            .method("POST")
            .uri("/api/file/init")
            .header("content-type", "application/json")
            .body(Body::from(init_body.to_string()))
            .unwrap();
        let resume_response = router.clone().oneshot(resume_request).await.unwrap();
        assert_eq!(resume_response.status(), StatusCode::OK);
        let body = resume_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["upload_id"], "resume-test");
        assert_eq!(json["received_chunks"], serde_json::json!([0]));

        let second_chunk = Request::builder()
            .method("PUT")
            .uri("/api/file/chunk/resume-test/1")
            .body(Body::from("def"))
            .unwrap();
        let second_response = router.clone().oneshot(second_chunk).await.unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);

        let complete_request = Request::builder()
            .method("POST")
            .uri("/api/file/complete/resume-test")
            .body(Body::empty())
            .unwrap();
        let complete_response = router.oneshot(complete_request).await.unwrap();
        assert_eq!(complete_response.status(), StatusCode::OK);

        let saved = std::fs::read_to_string(tmp_dir.path().join("resume.txt")).unwrap();
        assert_eq!(saved, "abcdef");
    }

    #[tokio::test]
    async fn test_concurrent_handle_file_init() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        let init_body = serde_json::json!({
            "sender_name": "concurrent-sender",
            "file_name": "concurrent.txt",
            "file_size": 100,
            "checksum": "0000000000000000000000000000000000000000000000000000000000000000",
            "chunk_size": 10,
            "upload_id": "concurrent-test",
        });

        let mut handles = Vec::new();
        for _ in 0..5 {
            let r = router.clone();
            let body_str = init_body.to_string();
            handles.push(tokio::spawn(async move {
                let request = axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/file/init")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body_str))
                    .unwrap();
                r.oneshot(request).await.unwrap()
            }));
        }

        for handle in handles {
            let response = handle.await.unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_stale_session_cleanup() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();

        let upload_sessions = Arc::new(Mutex::new(HashMap::new()));
        let _state = ServerState {
            registry,
            download_dir: tmp_dir.path().to_path_buf(),
            upload_sessions: upload_sessions.clone(),
            web_info: None,
            sse_tx: None,
        };

        let session_dir = tmp_dir.path().join(".dummy.chunks-stale-id");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();
        let dummy_path = tmp_dir.path().join("dummy.txt");
        tokio::fs::File::create(&dummy_path).await.unwrap();
        assert!(dummy_path.exists());

        let stale_session = UploadSession {
            sender_name: "stale-user".to_string(),
            final_path: dummy_path.clone(),
            temp_dir: session_dir.clone(),
            file_size: 1000,
            chunk_size: 100,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            received_chunks: BTreeSet::new(),
            last_active: std::time::Instant::now() - std::time::Duration::from_secs(7200), // 2 小时前
        };

        upload_sessions.lock().await.insert(
            "stale-id".to_string(),
            UploadSessionEntry::Active(stale_session),
        );
        assert!(session_dir.exists());

        cleanup_stale_upload_sessions(
            upload_sessions.clone(),
            std::time::Duration::from_secs(3600),
        )
        .await;

        assert!(!upload_sessions.lock().await.contains_key("stale-id"));
        assert!(
            !session_dir.exists(),
            "过期的隐藏临时文件夹应该已被彻底删除"
        );
        assert!(
            !dummy_path.exists(),
            "过期的隐藏临时占位文件应该已被彻底删除"
        );
    }

    #[tokio::test]
    async fn test_stale_session_cleanup_does_not_hold_lock_during_file_removal() {
        let tmp_dir = tempdir().unwrap();
        let upload_sessions = Arc::new(Mutex::new(HashMap::new()));
        let session_dir = tmp_dir.path().join(".dummy.chunks-stale-lock");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();
        let dummy_path = tmp_dir.path().join("dummy-lock.txt");
        tokio::fs::File::create(&dummy_path).await.unwrap();

        let stale_session = UploadSession {
            sender_name: "stale-user".to_string(),
            final_path: dummy_path,
            temp_dir: session_dir,
            file_size: 1000,
            chunk_size: 100,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            received_chunks: BTreeSet::new(),
            last_active: std::time::Instant::now() - std::time::Duration::from_secs(7200),
        };

        upload_sessions.lock().await.insert(
            "stale-lock".to_string(),
            UploadSessionEntry::Active(stale_session),
        );

        let cleanup_started = Arc::new(tokio::sync::Notify::new());
        let release_cleanup = Arc::new(tokio::sync::Notify::new());
        let sessions_for_cleanup = upload_sessions.clone();
        let sessions_for_assertion = upload_sessions.clone();
        let cleanup_started_for_task = cleanup_started.clone();
        let release_cleanup_for_task = release_cleanup.clone();

        let cleanup_task = tokio::spawn(async move {
            cleanup_stale_upload_sessions_with(
                sessions_for_cleanup,
                std::time::Duration::from_secs(3600),
                move |_session| {
                    let cleanup_started = cleanup_started_for_task.clone();
                    let release_cleanup = release_cleanup_for_task.clone();
                    let sessions_for_assertion = sessions_for_assertion.clone();
                    async move {
                        cleanup_started.notify_waiters();
                        let guard = tokio::time::timeout(
                            std::time::Duration::from_millis(100),
                            sessions_for_assertion.lock(),
                        )
                        .await
                        .expect(
                            "cleanup must not hold the upload_sessions lock while removing files",
                        );
                        drop(guard);
                        release_cleanup.notified().await;
                    }
                },
            )
            .await;
        });

        cleanup_started.notified().await;
        release_cleanup.notify_waiters();
        cleanup_task.await.unwrap();
        assert!(!upload_sessions.lock().await.contains_key("stale-lock"));
    }

    #[tokio::test]
    async fn test_file_chunk_limit() {
        let registry = PeerRegistry::new();
        let dir = tempdir().unwrap();
        let app = make_router(registry, dir.path().to_path_buf());

        // 直接通过 init 初始化一个 session
        let init_req = InitUploadRequest {
            sender_name: "test".to_string(),
            file_name: "large.bin".to_string(),
            file_size: 100 * 1024 * 1024,
            checksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            chunk_size: 8 * 1024 * 1024,
            upload_id: Some("test-limit-id".to_string()),
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/file/init")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&init_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 发送 33MB 的大分片数据 (通过伪造 content-length，发送 100 字节以节省内存)
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/file/chunk/test-limit-id/0")
                    .header("content-length", (33 * 1024 * 1024).to_string())
                    .body(Body::from(vec![0u8; 100]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_concurrent_available_path_reservation() {
        let registry = PeerRegistry::new();
        let dir = tempdir().unwrap();
        let download_dir = dir.path().to_path_buf();
        let app = make_router(registry, download_dir.clone());

        let payload1 = InitUploadRequest {
            sender_name: "client1".to_string(),
            file_name: "collision.txt".to_string(),
            file_size: 100,
            checksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            chunk_size: 100,
            upload_id: Some("id-1".to_string()),
        };
        let payload2 = InitUploadRequest {
            sender_name: "client2".to_string(),
            file_name: "collision.txt".to_string(),
            file_size: 100,
            checksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            chunk_size: 100,
            upload_id: Some("id-2".to_string()),
        };

        let fut1 = app.clone().oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/file/init")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload1).unwrap()))
                .unwrap(),
        );
        let fut2 = app.clone().oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/file/init")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload2).unwrap()))
                .unwrap(),
        );

        let (res1, res2) = tokio::join!(fut1, fut2);
        assert_eq!(res1.unwrap().status(), StatusCode::OK);
        assert_eq!(res2.unwrap().status(), StatusCode::OK);

        // 验证两个占位文件都存在，证明两个并发请求分配到了不同的文件名
        assert!(download_dir.join("collision.txt").exists());
        assert!(download_dir.join("collision_1.txt").exists());
    }

    #[tokio::test]
    async fn test_concurrent_chunk_write() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let router = make_router(registry, tmp_dir.path().to_path_buf());

        // Initialize upload session
        let upload_id = "concurrent-chunk-write-id";
        let chunk_data = b"hello chunk";
        let checksum = sha256_hex(chunk_data);
        let init_req = InitUploadRequest {
            sender_name: "test".to_string(),
            file_name: "concurrent_chunk.txt".to_string(),
            file_size: chunk_data.len() as u64,
            checksum,
            chunk_size: chunk_data.len() as u64,
            upload_id: Some(upload_id.to_string()),
        };

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/file/init")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&init_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Send two concurrent chunk upload PUT requests to /api/file/chunk/:upload_id/:index
        let req1 = Request::builder()
            .method("PUT")
            .uri(format!("/api/file/chunk/{}/0", upload_id))
            .body(Body::from(chunk_data.to_vec()))
            .unwrap();
        let req2 = Request::builder()
            .method("PUT")
            .uri(format!("/api/file/chunk/{}/0", upload_id))
            .body(Body::from(chunk_data.to_vec()))
            .unwrap();

        let router1 = router.clone();
        let router2 = router.clone();

        let fut1 = tokio::spawn(async move { router1.oneshot(req1).await.unwrap() });
        let fut2 = tokio::spawn(async move { router2.oneshot(req2).await.unwrap() });

        let (res1, res2) = tokio::join!(fut1, fut2);
        let status1 = res1.unwrap().status();
        let status2 = res2.unwrap().status();

        // Both requests should complete successfully
        assert_eq!(status1, StatusCode::OK);
        assert_eq!(status2, StatusCode::OK);

        // Verify that the final chunk file exists and has correct size and content
        let session_temp_dir = tmp_dir
            .path()
            .join(format!(".concurrent_chunk.txt.chunks-{}", upload_id));
        let chunk_file = session_temp_dir.join("chunk_0");
        assert!(chunk_file.exists());
        let saved_content = tokio::fs::read(&chunk_file).await.unwrap();
        assert_eq!(saved_content.len(), chunk_data.len());
        assert_eq!(saved_content, chunk_data.to_vec());
    }

    #[tokio::test]
    async fn test_cleanup_init_race() {
        let registry = PeerRegistry::new();
        let tmp_dir = tempdir().unwrap();
        let download_dir = tmp_dir.path().to_path_buf();

        // 1. 模拟旧版将分片删除放在锁外的逻辑（即存在竞态条件）
        let upload_id = "race-test-id".to_string();
        let basename = "race.txt";
        let temp_dir = chunk_upload_dir(&download_dir, basename, &upload_id);
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let chunk_file = chunk_path(&temp_dir, 0);
        tokio::fs::write(&chunk_file, b"data").await.unwrap();

        let final_path = download_dir.join(basename);
        tokio::fs::File::create(&final_path).await.unwrap();

        let stale_session = UploadSession {
            sender_name: "sender".to_string(),
            final_path: final_path.clone(),
            temp_dir: temp_dir.clone(),
            file_size: 4,
            chunk_size: 4,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            received_chunks: BTreeSet::new(),
            last_active: std::time::Instant::now() - std::time::Duration::from_secs(7200),
        };

        let sessions = Arc::new(Mutex::new(HashMap::new()));
        sessions
            .lock()
            .await
            .insert(upload_id.clone(), UploadSessionEntry::Active(stale_session));

        let (tx_removed, rx_removed) = tokio::sync::oneshot::channel::<UploadSession>();
        let (tx_inited, rx_inited) = tokio::sync::oneshot::channel::<UploadSession>();

        // 启动模拟旧版清理逻辑的 task
        let sessions_clone = sessions.clone();
        let cleanup_task = tokio::spawn(async move {
            let mut map = sessions_clone.lock().await;
            let session = match map.remove("race-test-id").unwrap() {
                UploadSessionEntry::Active(session) => session,
                UploadSessionEntry::Removing(_) => panic!("unexpected removing session"),
            };
            drop(map); // 锁在此处被释放了

            // 发送信号通知主线程：内存 map 已移除且锁已释放
            let _ = tx_removed.send(session);

            // 等待主线程 init 完成的信号
            let session = rx_inited.await.unwrap();

            // 开始物理删除
            let _ = tokio::fs::remove_dir_all(&session.temp_dir).await;
            if session.final_path.exists() {
                if let Ok(metadata) = tokio::fs::metadata(&session.final_path).await {
                    if metadata.len() == 0 {
                        let _ = tokio::fs::remove_file(&session.final_path).await;
                    }
                }
            }
        });

        // 等待清理线程把 map 移除并释放锁
        let session = rx_removed.await.unwrap();

        let state = ServerState {
            registry: registry.clone(),
            download_dir: download_dir.clone(),
            upload_sessions: sessions.clone(),
            web_info: None,
            sse_tx: None,
        };

        let payload = InitUploadRequest {
            sender_name: "sender".to_string(),
            file_name: basename.to_string(),
            file_size: 4,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            chunk_size: 4,
            upload_id: Some(upload_id.clone()),
        };

        // 客户端重新请求 init，此时会扫描并获取残留的分片
        let _response =
            handle_file_init(axum::extract::State(state.clone()), Ok(axum::Json(payload))).await;

        // 通知清理线程已完成 init，可以开始物理删除了
        let _ = tx_inited.send(session);

        cleanup_task.await.unwrap();

        // 验证竞态的发生：Session 中记录了分片 [0]，但磁盘上该分片却已被清理线程删除！
        let final_sessions = sessions.lock().await;
        let active_session = match final_sessions.get("race-test-id").unwrap() {
            UploadSessionEntry::Active(session) => session,
            UploadSessionEntry::Removing(_) => panic!("unexpected removing session"),
        };
        assert!(active_session.received_chunks.contains(&0));
        assert!(!chunk_file.exists());

        drop(final_sessions);

        // 2. 模拟新版将分片删除放在锁内的逻辑（竞态被消除）
        let upload_id_safe = "race-test-id-safe".to_string();
        let temp_dir_safe = chunk_upload_dir(&download_dir, basename, &upload_id_safe);
        tokio::fs::create_dir_all(&temp_dir_safe).await.unwrap();

        let chunk_file_safe = chunk_path(&temp_dir_safe, 0);
        tokio::fs::write(&chunk_file_safe, b"data").await.unwrap();

        let final_path_safe = download_dir.join(format!("{}.safe", basename));
        tokio::fs::File::create(&final_path_safe).await.unwrap();

        let stale_session_safe = UploadSession {
            sender_name: "sender".to_string(),
            final_path: final_path_safe.clone(),
            temp_dir: temp_dir_safe.clone(),
            file_size: 4,
            chunk_size: 4,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            received_chunks: BTreeSet::new(),
            last_active: std::time::Instant::now() - std::time::Duration::from_secs(7200),
        };

        let sessions_safe = Arc::new(Mutex::new(HashMap::new()));
        sessions_safe.lock().await.insert(
            upload_id_safe.clone(),
            UploadSessionEntry::Active(stale_session_safe),
        );

        let cleanup_started = Arc::new(tokio::sync::Notify::new());
        let release_cleanup = Arc::new(tokio::sync::Notify::new());

        // 启动新版清理逻辑：锁内只标记 Removing，物理删除在锁外执行。
        let sessions_safe_clone = sessions_safe.clone();
        let cleanup_started_for_task = cleanup_started.clone();
        let release_cleanup_for_task = release_cleanup.clone();
        let cleanup_task_safe = tokio::spawn(async move {
            cleanup_stale_upload_sessions_with(
                sessions_safe_clone,
                std::time::Duration::from_secs(3600),
                move |session| {
                    let cleanup_started = cleanup_started_for_task.clone();
                    let release_cleanup = release_cleanup_for_task.clone();
                    async move {
                        cleanup_started.notify_waiters();
                        release_cleanup.notified().await;
                        remove_upload_session_files(session).await;
                    }
                },
            )
            .await;
        });

        // 等待清理线程进入物理删除阶段，此时全局 map 锁已经释放，但该 upload_id 仍有 Removing tombstone。
        cleanup_started.notified().await;

        let state_safe = ServerState {
            registry: registry.clone(),
            download_dir: download_dir.clone(),
            upload_sessions: sessions_safe.clone(),
            web_info: None,
            sse_tx: None,
        };

        let payload_safe = InitUploadRequest {
            sender_name: "sender".to_string(),
            file_name: format!("{}.safe", basename),
            file_size: 4,
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
            chunk_size: 4,
            upload_id: Some(upload_id_safe.clone()),
        };

        // 客户端请求 init 动作不会被全局锁阻塞，但会等待同 upload_id 的 tombstone 清理完成。
        let init_task = tokio::spawn(async move {
            handle_file_init(
                axum::extract::State(state_safe),
                Ok(axum::Json(payload_safe)),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !init_task.is_finished(),
            "init should wait for cleanup of the same upload_id before scanning chunks"
        );

        release_cleanup.notify_waiters();
        let _response_safe = init_task.await.unwrap();

        cleanup_task_safe.await.unwrap();

        // 验证：新注册的 Session 中 received_chunks 是空的，避免了不一致的发生
        let final_sessions_safe = sessions_safe.lock().await;
        let active_session_safe = match final_sessions_safe.get("race-test-id-safe").unwrap() {
            UploadSessionEntry::Active(session) => session,
            UploadSessionEntry::Removing(_) => panic!("unexpected removing session"),
        };
        assert!(active_session_safe.received_chunks.is_empty());
        assert!(!chunk_file_safe.exists());
    }
}

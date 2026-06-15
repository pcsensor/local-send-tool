use axum::{
    response::{
        sse::{Event, KeepAlive, Sse},
        Html,
    },
    Json,
};
use futures_util::stream::Stream;
use serde::Serialize;
use std::convert::Infallible;

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone, Debug, Serialize)]
pub struct WebRuntimeInfo {
    pub node_name: String,
    pub port: u16,
    pub bind_ip: Option<String>,
    pub download_dir: String,
    pub version: &'static str,
    pub ui_stack: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compress: Option<crate::client::CompressionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_concurrency: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SseMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub sender: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
}

impl SseMessage {
    pub fn message(sender: String, text: String) -> Self {
        Self {
            event_type: "message".to_string(),
            sender,
            text: Some(text),
            file_name: None,
            file_size: None,
        }
    }

    pub fn file(sender: String, file_name: String, file_size: u64) -> Self {
        Self {
            event_type: "file".to_string(),
            sender,
            text: None,
            file_name: Some(file_name),
            file_size: Some(file_size),
        }
    }
}

pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

pub async fn runtime_info(info: WebRuntimeInfo) -> Json<WebRuntimeInfo> {
    Json(info)
}

pub async fn sse_events(
    mut rx: tokio::sync::broadcast::Receiver<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    yield Ok(Event::default().data(msg));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

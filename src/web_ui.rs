use axum::{response::Html, Json};
use serde::Serialize;

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone, Debug, Serialize)]
pub struct WebRuntimeInfo {
    pub node_name: String,
    pub port: u16,
    pub bind_ip: Option<String>,
    pub download_dir: String,
    pub version: &'static str,
    pub ui_stack: &'static str,
}

pub async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

pub async fn runtime_info(info: WebRuntimeInfo) -> Json<WebRuntimeInfo> {
    Json(info)
}

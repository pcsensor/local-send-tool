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
const WEB_TOKEN_PLACEHOLDER: &str = "__LAN_SHARE_WEB_TOKEN__";

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

pub async fn index() -> Html<String> {
    Html(index_html_with_token(None))
}

pub fn index_html_with_token(web_token: Option<&str>) -> String {
    INDEX_HTML.replace(WEB_TOKEN_PLACEHOLDER, web_token.unwrap_or(""))
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

#[cfg(test)]
mod tests {
    use super::INDEX_HTML;

    fn css_rule(selector: &str) -> &str {
        let selector = format!("{selector} {{");
        let start = INDEX_HTML
            .find(&selector)
            .unwrap_or_else(|| panic!("missing CSS selector: {selector}"));
        let after_selector = &INDEX_HTML[start..];
        let open = after_selector
            .find('{')
            .unwrap_or_else(|| panic!("missing CSS block for selector: {selector}"));
        let after_open = &after_selector[open + 1..];
        let close = after_open
            .find('}')
            .unwrap_or_else(|| panic!("unterminated CSS block for selector: {selector}"));
        &after_open[..close]
    }

    #[test]
    fn chat_layout_keeps_composer_visible_and_history_scrollable() {
        let app_rule = css_rule(".app");
        assert!(
            app_rule.contains("height: 100dvh"),
            "the app shell must stay bound to the viewport"
        );
        assert!(
            app_rule.contains("overflow: hidden"),
            "page overflow should not push the composer off-screen"
        );
        assert!(
            app_rule.contains("grid-template-rows: minmax(0, 1fr)"),
            "the app grid row must not expand to fit tall side panels"
        );

        let chats_rule = css_rule(".chats");
        assert!(
            chats_rule.contains("min-height: 0"),
            "the chats sidebar must not increase the app grid height"
        );

        let chat_rule = css_rule(".chat");
        assert!(
            chat_rule.contains("min-height: 0"),
            "the chat grid item must be allowed to shrink inside the viewport"
        );
        assert!(
            chat_rule.contains("overflow: hidden"),
            "only the message history should scroll, not the whole chat panel"
        );

        let info_rule = css_rule(".info");
        assert!(
            info_rule.contains("min-height: 0"),
            "the info sidebar must not increase the app grid height"
        );

        let messages_rule = css_rule(".messages");
        assert!(
            messages_rule.contains("overflow-y: auto"),
            "message history should remain vertically scrollable"
        );

        let mobile_rule_start = INDEX_HTML
            .find("@media (max-width: 760px)")
            .expect("missing mobile layout rule");
        let style_end = INDEX_HTML[mobile_rule_start..]
            .find("</style>")
            .expect("missing style close tag");
        let mobile_rules = &INDEX_HTML[mobile_rule_start..mobile_rule_start + style_end];
        assert!(
            !mobile_rules.contains("overflow: auto"),
            "mobile layout must not fall back to document scrolling"
        );
        assert!(
            mobile_rules.contains("height: 100dvh"),
            "mobile layout must keep the composer at the bottom of the viewport"
        );
    }

    #[test]
    fn web_chat_keeps_group_and_private_histories_separate() {
        assert!(
            INDEX_HTML.contains("conversations:"),
            "web UI should keep message history in per-conversation state"
        );
        assert!(
            INDEX_HTML.contains("function currentConversationId()"),
            "web UI should derive a stable conversation id from the selected chat"
        );
        assert!(
            INDEX_HTML.contains("function renderMessages({ scroll = \"preserve\" } = {})"),
            "switching chats should render only the selected conversation history"
        );
        assert!(
            INDEX_HTML.contains("appendConversationMessage(currentConversationId()"),
            "locally sent messages should be appended to the active conversation"
        );
        assert!(
            INDEX_HTML.contains("conversationIdForSender"),
            "received messages should be routed to the sender's private conversation"
        );
    }

    #[test]
    fn web_chat_preserves_manual_message_scroll() {
        assert!(
            INDEX_HTML.contains("const MESSAGE_BOTTOM_THRESHOLD"),
            "web UI should define a near-bottom threshold for sticky scrolling"
        );
        assert!(
            INDEX_HTML.contains("function renderAll({ messageScroll = \"preserve\" } = {})"),
            "regular UI refreshes should preserve the current message scroll"
        );
        assert!(
            INDEX_HTML.contains("renderMessages({ scroll: messageScroll })"),
            "renderAll should pass the selected scroll mode into message rendering"
        );
        assert!(
            INDEX_HTML.contains("container.scrollTop = previousScrollTop"),
            "preserve mode should keep the user's current history position"
        );
        assert!(
            INDEX_HTML.contains("renderAll({ messageScroll: \"bottom\" })"),
            "explicit chat switches should still open at the latest message"
        );
        assert!(
            !INDEX_HTML.contains("scrollIntoView"),
            "message rendering must not unconditionally scroll to the newest message"
        );
    }

    #[test]
    fn config_modal_uses_runtime_defaults_for_all_fields() {
        assert!(
            INDEX_HTML.contains("function runtimeConfigValues(info)"),
            "web UI should map runtime info into config form values"
        );
        assert!(
            INDEX_HTML.contains("applyConfigValues(runtimeConfigValues(info))"),
            "runtime loading should populate the config modal before /api/config is available"
        );
        assert!(
            INDEX_HTML.contains("applyConfigValues(state.configDefaults)"),
            "local config loading should reuse the same config form mapping"
        );

        for field in [
            "download_dir",
            "port",
            "node_name",
            "bind_ip",
            "retry",
            "compress",
            "chunked",
            "chunk_size",
            "chunk_concurrency",
            "cancel_timeout",
            "concurrency",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("info.{field}")),
                "runtime config mapping should include {field}"
            );
        }

        for input_id in [
            "cfg-download-dir",
            "cfg-port",
            "cfg-name",
            "cfg-bind-ip",
            "cfg-retry",
            "cfg-compress",
            "cfg-chunked",
            "cfg-chunk-size",
            "cfg-chunk-concurrency",
            "cfg-cancel-timeout",
            "cfg-concurrency",
        ] {
            assert!(
                INDEX_HTML.contains(&format!("$(\"{input_id}\").value")),
                "config modal should populate {input_id}"
            );
        }
    }

    #[test]
    fn web_api_requests_include_runtime_token() {
        assert!(
            INDEX_HTML.contains("const WEB_API_TOKEN = \"__LAN_SHARE_WEB_TOKEN__\""),
            "web UI should receive a server-generated API token"
        );
        assert!(
            INDEX_HTML.contains("function webApiHeaders(headers = {})"),
            "web UI should centralize protected Web API headers"
        );
        assert!(
            INDEX_HTML.contains("result[\"x-lan-share-web-token\"] = WEB_API_TOKEN"),
            "protected Web API calls should include the token header"
        );
        assert!(
            INDEX_HTML
                .contains("headers: webApiHeaders({ \"content-type\": \"application/json\" })"),
            "message sends should use protected Web API headers"
        );
        assert!(
            INDEX_HTML.contains("xhr.setRequestHeader(\"x-lan-share-web-token\", WEB_API_TOKEN)"),
            "file sends should use protected Web API headers"
        );
    }

    #[test]
    fn rendered_index_replaces_web_api_token_placeholder() {
        let html = super::index_html_with_token(Some("token-123"));
        assert!(html.contains("const WEB_API_TOKEN = \"token-123\""));
        assert!(!html.contains("__LAN_SHARE_WEB_TOKEN__"));
    }

    #[test]
    fn config_modal_preserves_unedited_defaults_and_marks_remote_readonly() {
        assert!(
            INDEX_HTML.contains("configDefaults: {}"),
            "web UI should keep the loaded config defaults while editing"
        );
        assert!(
            INDEX_HTML.contains("...state.configDefaults"),
            "saving should preserve config fields that are not shown in the modal"
        );
        assert!(
            INDEX_HTML.contains("function optionalIntegerValue(id)"),
            "numeric config parsing should preserve valid zero values instead of using || null"
        );
        assert!(
            !INDEX_HTML.contains("progress: true"),
            "saving from the modal must not force progress=true when progress is not editable"
        );
        assert!(
            INDEX_HTML.contains("setConfigSaveAvailable(canSave)"),
            "remote config views should disable saving when /api/config is unavailable"
        );
        assert!(
            INDEX_HTML.contains("当前访问无法保存配置，请在本机打开 Web UI 后修改。"),
            "remote config views should explain why saving is unavailable"
        );
    }
}

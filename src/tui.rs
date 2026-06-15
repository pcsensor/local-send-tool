use crate::client::{
    send_file_with_options, send_text, CompressionMode, FileSendOptions, ProgressMode,
};
use crate::config::{
    config_file_path, expand_tilde_path, user_home_dir, AppConfig, ConfigDefaults,
};
use crate::discovery::{get_local_ips, start_broadcaster, start_listener};
use crate::peer::{Peer, PeerRegistry};
use crate::server::make_router_with_events;
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table},
    Frame, Terminal,
};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// ── Data types ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub sender: String,
    pub text: String,
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub is_self: bool,
    pub is_file: bool,
    pub file_name: Option<String>,
    pub file_size: Option<u64>,
    pub is_system: bool,
}

#[derive(Debug, Clone)]
pub enum TuiEvent {
    MessageReceived {
        sender: String,
        text: String,
    },
    FileReceived {
        sender: String,
        file_name: String,
        file_size: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ActivePanel {
    Peers,
    Chat,
    Settings,
}

#[derive(Debug, Clone, PartialEq)]
enum InputMode {
    Normal,
    Editing,
    FilePicker,
    SettingsEdit,
}

const SERVER_RAIL: Color = Color::Rgb(30, 31, 34);
const CHANNEL_BG: Color = Color::Rgb(43, 45, 49);
const CHANNEL_HOVER: Color = Color::Rgb(53, 55, 60);
const CHAT_BG: Color = Color::Rgb(49, 51, 56);
const CHAT_INPUT: Color = Color::Rgb(56, 58, 64);
const BORDER: Color = Color::Rgb(30, 31, 34);
const TEXT: Color = Color::Rgb(219, 222, 225);
const MUTED: Color = Color::Rgb(148, 155, 164);
const BRAND: Color = Color::Rgb(88, 101, 242);
const GREEN: Color = Color::Rgb(35, 165, 89);

// ── Settings state ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SettingsState {
    download_dir: String,
    port: u16,
    name: String,
    bind_ip: String,
    retry: usize,
    compress: String,
    chunked: bool,
    chunk_size: u64,
    chunk_concurrency: usize,
    cancel_timeout: u64,
    selected_field: usize,
    edit_buffer: Option<String>,
    saved_indicator: Option<Instant>,
}

impl SettingsState {
    fn from_config(config: &AppConfig) -> Self {
        let d = &config.defaults;
        Self {
            download_dir: d
                .download_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from("./downloads"))
                .to_string_lossy()
                .to_string(),
            port: d.port.unwrap_or(8080),
            name: d.name.clone().unwrap_or_else(|| {
                hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "Unknown".to_string())
            }),
            bind_ip: d.bind_ip.clone().unwrap_or_default(),
            retry: d.retry.unwrap_or(0),
            compress: d.compress.unwrap_or(CompressionMode::Auto).to_string(),
            chunked: d.chunked.unwrap_or(false),
            chunk_size: d.chunk_size.unwrap_or(8 * 1024 * 1024),
            chunk_concurrency: d.chunk_concurrency.unwrap_or(4),
            cancel_timeout: d.cancel_timeout.unwrap_or(10),
            selected_field: 0,
            edit_buffer: None,
            saved_indicator: None,
        }
    }

    fn to_config_defaults(&self) -> ConfigDefaults {
        ConfigDefaults {
            download_dir: Some(PathBuf::from(&self.download_dir)),
            port: Some(self.port),
            name: Some(self.name.clone()),
            bind_ip: if self.bind_ip.is_empty() {
                None
            } else {
                Some(self.bind_ip.clone())
            },
            retry: Some(self.retry),
            compress: Some(self.compress.parse().unwrap_or(CompressionMode::Auto)),
            chunked: Some(self.chunked),
            chunk_size: Some(self.chunk_size),
            chunk_concurrency: Some(self.chunk_concurrency),
            cancel_timeout: Some(self.cancel_timeout),
            progress: Some(true),
            concurrency: Some(3),
        }
    }

    fn save(&self) -> io::Result<()> {
        let config_path = config_file_path()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "无法确定配置文件路径"))?;
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let app_config = AppConfig {
            defaults: self.to_config_defaults(),
        };
        let content = toml::to_string_pretty(&app_config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(&config_path, content)?;
        Ok(())
    }

    fn field_labels() -> Vec<&'static str> {
        vec![
            "下载目录",
            "端口",
            "节点名称",
            "绑定IP",
            "重试次数",
            "压缩模式",
            "分片上传",
            "分片大小",
            "分片并发",
            "取消超时",
        ]
    }

    fn get_field_value(&self, idx: usize) -> String {
        match idx {
            0 => self.download_dir.clone(),
            1 => self.port.to_string(),
            2 => self.name.clone(),
            3 => {
                if self.bind_ip.is_empty() {
                    "(默认)".to_string()
                } else {
                    self.bind_ip.clone()
                }
            }
            4 => self.retry.to_string(),
            5 => self.compress.clone(),
            6 => self.chunked.to_string(),
            7 => format_bytes(self.chunk_size),
            8 => self.chunk_concurrency.to_string(),
            9 => format!("{}秒", self.cancel_timeout),
            _ => String::new(),
        }
    }

    fn set_field_value(&mut self, idx: usize, value: &str) -> bool {
        match idx {
            0 => {
                self.download_dir = value.to_string();
                true
            }
            1 => {
                if let Ok(v) = value.parse() {
                    self.port = v;
                    true
                } else {
                    false
                }
            }
            2 => {
                self.name = value.to_string();
                true
            }
            3 => {
                self.bind_ip = value.to_string();
                true
            }
            4 => {
                if let Ok(v) = value.parse() {
                    self.retry = v;
                    true
                } else {
                    false
                }
            }
            5 => {
                if value.parse::<CompressionMode>().is_ok() {
                    self.compress = value.to_string();
                    true
                } else {
                    false
                }
            }
            6 => {
                if let Ok(v) = value.parse::<bool>() {
                    self.chunked = v;
                    true
                } else {
                    false
                }
            }
            7 => {
                if let Some(v) = parse_size(value) {
                    self.chunk_size = v;
                    true
                } else {
                    false
                }
            }
            8 => {
                if let Ok(v) = value.parse() {
                    self.chunk_concurrency = v;
                    true
                } else {
                    false
                }
            }
            9 => {
                if let Ok(v) = value.parse() {
                    self.cancel_timeout = v;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// ── App state ───────────────────────────────────────────────────────

struct App {
    peers: Vec<Peer>,
    selected_peer: Option<usize>,
    messages: HashMap<String, Vec<ChatMessage>>,
    input_mode: InputMode,
    input_buffer: String,
    active_panel: ActivePanel,
    settings: SettingsState,
    status_message: Option<(String, Instant)>,
    message_scroll_offset: u16,
    should_stick_to_bottom: bool,
    show_file_picker: bool,
    last_peer_update: Instant,
    node_name: String,
    registry: PeerRegistry,
    event_rx: mpsc::UnboundedReceiver<TuiEvent>,
    event_tx: mpsc::UnboundedSender<TuiEvent>,
}

impl App {
    fn new(config: &AppConfig) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let node_name = config.defaults.name.clone().unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "Unknown".to_string())
        });
        Self {
            peers: Vec::new(),
            selected_peer: None,
            messages: HashMap::new(),
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            active_panel: ActivePanel::Peers,
            settings: SettingsState::from_config(config),
            status_message: None,
            message_scroll_offset: 0,
            should_stick_to_bottom: true,
            show_file_picker: false,
            last_peer_update: Instant::now(),
            node_name,
            registry: PeerRegistry::new(),
            event_rx,
            event_tx,
        }
    }

    fn get_selected_peer(&self) -> Option<&Peer> {
        self.selected_peer.and_then(|i| self.peers.get(i))
    }

    fn add_message(&mut self, peer_uuid: &str, msg: ChatMessage) {
        self.messages
            .entry(peer_uuid.to_string())
            .or_default()
            .push(msg);
        self.message_scroll_offset = 0;
        self.should_stick_to_bottom = true;
    }

    fn add_system_message(&mut self, peer_uuid: &str, text: String) {
        self.messages
            .entry(peer_uuid.to_string())
            .or_default()
            .push(ChatMessage {
                sender: String::new(),
                text,
                timestamp: chrono::Local::now(),
                is_self: false,
                is_file: false,
                file_name: None,
                file_size: None,
                is_system: true,
            });
        self.message_scroll_offset = 0;
        self.should_stick_to_bottom = true;
    }

    fn set_status(&mut self, msg: String) {
        self.status_message = Some((msg, Instant::now()));
    }

    fn clear_status_if_expired(&mut self) {
        if let Some((_, time)) = self.status_message {
            if time.elapsed() > Duration::from_secs(3) {
                self.status_message = None;
            }
        }
        if let Some(time) = self.settings.saved_indicator {
            if time.elapsed() > Duration::from_secs(2) {
                self.settings.saved_indicator = None;
            }
        }
    }

    fn update_peers(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_peer_update) < Duration::from_millis(100) {
            return;
        }
        self.last_peer_update = now;
        let prev_uuid = self
            .selected_peer
            .and_then(|i| self.peers.get(i))
            .map(|p| p.uuid.clone());
        self.peers = self.registry.list();
        self.selected_peer = prev_uuid
            .as_ref()
            .and_then(|uuid| self.peers.iter().position(|p| &p.uuid == uuid))
            .or_else(|| (!self.peers.is_empty()).then_some(0));
    }

    fn process_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                TuiEvent::MessageReceived { sender, text } => {
                    // Find peer by sender name, add to that peer's chat
                    let peer_uuid = self
                        .peers
                        .iter()
                        .find(|p| p.name == sender)
                        .map(|p| p.uuid.clone())
                        .unwrap_or_else(|| sender.clone());
                    self.add_message(
                        &peer_uuid,
                        ChatMessage {
                            sender: sender.clone(),
                            text,
                            timestamp: chrono::Local::now(),
                            is_self: false,
                            is_file: false,
                            file_name: None,
                            file_size: None,
                            is_system: false,
                        },
                    );
                }
                TuiEvent::FileReceived {
                    sender,
                    file_name,
                    file_size,
                } => {
                    let peer_uuid = self
                        .peers
                        .iter()
                        .find(|p| p.name == sender)
                        .map(|p| p.uuid.clone())
                        .unwrap_or_else(|| sender.clone());
                    self.add_message(
                        &peer_uuid,
                        ChatMessage {
                            sender: sender.clone(),
                            text: String::new(),
                            timestamp: chrono::Local::now(),
                            is_self: false,
                            is_file: true,
                            file_name: Some(file_name.clone()),
                            file_size: Some(file_size),
                            is_system: false,
                        },
                    );
                    self.add_system_message(
                        &peer_uuid,
                        format!("已接收文件: {} ({})", file_name, format_bytes(file_size)),
                    );
                }
            }
        }
    }
}

// ── Entry point ─────────────────────────────────────────────────────

pub async fn run_tui(
    config: AppConfig,
    bind_ip: Option<std::net::Ipv4Addr>,
    port: u16,
) -> io::Result<()> {
    // Setup terminal
    enable_raw_mode().map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound || e.raw_os_error() == Some(6) {
            io::Error::new(
                io::ErrorKind::Other,
                "无法初始化终端：请确保在交互式终端（TTY）中运行此命令，不支持管道或后台模式",
            )
        } else {
            io::Error::new(e.kind(), format!("终端初始化失败: {}", e))
        }
    })?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .map_err(|e| io::Error::new(e.kind(), format!("terminal setup failed: {}", e)))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)
        .map_err(|e| io::Error::new(e.kind(), format!("terminal creation failed: {}", e)))?;

    let app_registry = PeerRegistry::new();

    // Create app first to get event channel
    let mut app = App::new(&config);
    app.registry = app_registry.clone();

    // Start server with event channel for TUI
    let download_dir = config
        .defaults
        .download_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./downloads"));
    let home = user_home_dir().unwrap_or_else(|| PathBuf::from("."));
    let download_dir = expand_tilde_path(download_dir, &home);
    let router = make_router_with_events(
        app_registry.clone(),
        download_dir,
        Some(app.event_tx.clone()),
        None,
    );

    // Try to bind TCP listener
    let (listener, actual_port) = match crate::find_available_port(bind_ip, port).await {
        Ok(result) => result,
        Err(e) => {
            let ip_str = bind_ip
                .map(|ip| ip.to_string())
                .unwrap_or("0.0.0.0".to_string());
            return Err(io::Error::new(
                e.kind(),
                format!("TCP bind failed (bind_ip={}): {}", ip_str, e),
            ));
        }
    };
    let server_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("Server error: {}", e);
        }
    });

    let node_name = config.defaults.name.clone().unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "Unknown".to_string())
    });

    let peer = Peer {
        uuid: uuid::Uuid::new_v4().to_string(),
        name: node_name.clone(),
        port: actual_port,
        ips: get_local_ips(bind_ip),
    };

    app.node_name = peer.name.clone();

    // Start peer discovery - errors are logged but don't crash TUI
    let listener_registry = app_registry.clone();
    tokio::spawn(async move {
        if let Err(e) = start_listener(listener_registry, bind_ip).await {
            eprintln!("Discovery listener error: {}", e);
        }
    });
    let broadcaster_peer = peer.clone();
    tokio::spawn(async move {
        if let Err(e) = start_broadcaster(broadcaster_peer, bind_ip).await {
            eprintln!("Discovery broadcaster error: {}", e);
        }
    });

    // Start stale peer cleanup
    let clean_registry = app_registry.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            clean_registry.clean_stale(Duration::from_secs(9));
        }
    });

    let result = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    // Print server info on exit
    println!(
        "LAN Share TUI 已退出 (服务运行于 http://{} 端口 {})",
        server_addr.ip(),
        server_addr.port()
    );

    result
}

async fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
) -> io::Result<()> {
    loop {
        app.update_peers();
        app.process_events();
        terminal.draw(|f| ui(f, app))?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if handle_key_event(app, key) {
                    return Ok(());
                }
            }
        }

        app.clear_status_if_expired();
    }
}

// ── Key handling ────────────────────────────────────────────────────

fn handle_key_event(app: &mut App, key: KeyEvent) -> bool {
    match app.input_mode {
        InputMode::Normal => handle_normal_mode(app, key),
        InputMode::Editing => handle_editing_mode(app, key),
        InputMode::FilePicker => handle_file_picker_mode(app, key),
        InputMode::SettingsEdit => handle_settings_edit_mode(app, key),
    }
}

fn handle_normal_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL)
        && key.code == KeyCode::Char('s')
        && app.active_panel == ActivePanel::Settings
    {
        save_settings(app);
        return false;
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return true,
        KeyCode::Tab => {
            app.active_panel = match app.active_panel {
                ActivePanel::Peers => ActivePanel::Chat,
                ActivePanel::Chat => ActivePanel::Settings,
                ActivePanel::Settings => ActivePanel::Peers,
            };
        }
        KeyCode::BackTab => {
            app.active_panel = match app.active_panel {
                ActivePanel::Peers => ActivePanel::Settings,
                ActivePanel::Chat => ActivePanel::Peers,
                ActivePanel::Settings => ActivePanel::Chat,
            };
        }
        KeyCode::Char('i') | KeyCode::Enter
            if app.active_panel == ActivePanel::Chat && app.selected_peer.is_some() =>
        {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('f')
            if app.active_panel == ActivePanel::Chat && app.selected_peer.is_some() =>
        {
            app.show_file_picker = true;
            app.input_mode = InputMode::FilePicker;
            app.input_buffer.clear();
        }
        KeyCode::Char('s') => {
            app.active_panel = ActivePanel::Settings;
        }
        KeyCode::Enter if app.active_panel == ActivePanel::Peers && app.selected_peer.is_some() => {
            app.active_panel = ActivePanel::Chat;
        }
        KeyCode::Enter if app.active_panel == ActivePanel::Settings => {
            let field = app.settings.selected_field;
            app.settings.edit_buffer = Some(app.settings.get_field_value(field));
            app.input_mode = InputMode::SettingsEdit;
        }
        KeyCode::Up => match app.active_panel {
            ActivePanel::Peers => {
                app.selected_peer = previous_peer_selection(app.selected_peer, app.peers.len());
            }
            ActivePanel::Chat => {
                app.should_stick_to_bottom = false;
                app.message_scroll_offset = app.message_scroll_offset.saturating_add(3);
            }
            ActivePanel::Settings => {
                if app.settings.selected_field > 0 {
                    app.settings.selected_field -= 1;
                }
            }
        },
        KeyCode::Down => match app.active_panel {
            ActivePanel::Peers => {
                app.selected_peer = next_peer_selection(app.selected_peer, app.peers.len());
            }
            ActivePanel::Chat => {
                if app.message_scroll_offset <= 3 {
                    app.message_scroll_offset = 0;
                    app.should_stick_to_bottom = true;
                } else {
                    app.message_scroll_offset = app.message_scroll_offset.saturating_sub(3);
                }
            }
            ActivePanel::Settings => {
                if app.settings.selected_field < 9 {
                    app.settings.selected_field += 1;
                }
            }
        },
        _ => {}
    }
    false
}

fn handle_editing_mode(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.input_buffer.clear();
        }
        KeyCode::Enter => {
            if !app.input_buffer.is_empty() && app.selected_peer.is_some() {
                let peer = app.peers[app.selected_peer.unwrap()].clone();
                let text = app.input_buffer.clone();
                let sender = app.node_name.clone();
                let peer_uuid = peer.uuid.clone();

                app.add_message(
                    &peer_uuid,
                    ChatMessage {
                        sender: sender.clone(),
                        text: text.clone(),
                        timestamp: chrono::Local::now(),
                        is_self: true,
                        is_file: false,
                        file_name: None,
                        file_size: None,
                        is_system: false,
                    },
                );

                tokio::spawn(async move {
                    let addr = if let Some(ip) = peer.ips.first() {
                        format!("{}:{}", ip, peer.port)
                    } else {
                        return;
                    };
                    let _ = send_text(&addr, &sender, &text).await;
                });

                app.input_buffer.clear();
                app.input_mode = InputMode::Normal;
                app.set_status("消息已发送".to_string());
            }
        }
        KeyCode::Char(c) => {
            app.input_buffer.push(c);
        }
        KeyCode::Backspace => {
            app.input_buffer.pop();
        }
        _ => {}
    }
    false
}

fn handle_file_picker_mode(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.show_file_picker = false;
            app.input_mode = InputMode::Normal;
            app.input_buffer.clear();
        }
        KeyCode::Enter => {
            if !app.input_buffer.is_empty() {
                let home = user_home_dir().unwrap_or_else(|| PathBuf::from("."));
                let path = expand_tilde_path(PathBuf::from(&app.input_buffer), &home);
                if path.exists() && path.is_file() {
                    app.show_file_picker = false;
                    app.input_mode = InputMode::Normal;

                    if let Some(peer_idx) = app.selected_peer {
                        let peer = app.peers[peer_idx].clone();
                        let sender = app.node_name.clone();
                        let peer_uuid = peer.uuid.clone();
                        let file_name = path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string();
                        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                        app.add_message(
                            &peer_uuid,
                            ChatMessage {
                                sender: sender.clone(),
                                text: String::new(),
                                timestamp: chrono::Local::now(),
                                is_self: true,
                                is_file: true,
                                file_name: Some(file_name.clone()),
                                file_size: Some(file_size),
                                is_system: false,
                            },
                        );

                        let options = FileSendOptions {
                            progress: ProgressMode::None,
                            ..FileSendOptions::default()
                        };

                        tokio::spawn(async move {
                            let addr = if let Some(ip) = peer.ips.first() {
                                format!("{}:{}", ip, peer.port)
                            } else {
                                return;
                            };
                            match send_file_with_options(&addr, &sender, &path, options).await {
                                Ok(()) => {}
                                Err(e) => eprintln!("File send failed: {}", e),
                            }
                        });

                        app.set_status(format!("正在发送: {}", file_name));
                    }

                    app.input_buffer.clear();
                } else {
                    app.set_status("文件不存在或不是有效文件".to_string());
                }
            }
        }
        KeyCode::Char(c) => {
            app.input_buffer.push(c);
        }
        KeyCode::Backspace => {
            app.input_buffer.pop();
        }
        _ => {}
    }
    false
}

fn handle_settings_edit_mode(app: &mut App, key: KeyEvent) -> bool {
    // First check Ctrl+S for save
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
        save_settings(app);
        return false;
    }

    // Handle editing
    match key.code {
        KeyCode::Esc => {
            app.settings.edit_buffer = None;
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Enter => {
            let field = app.settings.selected_field;
            let buf = app.settings.edit_buffer.clone().unwrap_or_default();
            if app.settings.set_field_value(field, &buf) {
                app.settings.edit_buffer = None;
                app.input_mode = InputMode::Normal;
                app.set_status("配置已修改 (Ctrl+S 保存到 config.toml)".to_string());
            } else {
                app.set_status("无效的值".to_string());
            }
        }
        KeyCode::Char(c) => {
            if let Some(buf) = &mut app.settings.edit_buffer {
                buf.push(c);
            }
        }
        KeyCode::Backspace => {
            if let Some(buf) = &mut app.settings.edit_buffer {
                buf.pop();
            }
        }
        _ => {}
    }
    false
}

fn previous_peer_selection(current: Option<usize>, len: usize) -> Option<usize> {
    match (current, len) {
        (_, 0) => None,
        (None, _) => Some(0),
        (Some(0), _) => Some(0),
        (Some(index), _) => Some(index - 1),
    }
}

fn next_peer_selection(current: Option<usize>, len: usize) -> Option<usize> {
    match (current, len) {
        (_, 0) => None,
        (None, _) => Some(0),
        (Some(index), len) if index + 1 < len => Some(index + 1),
        (Some(index), _) => Some(index),
    }
}

fn save_settings(app: &mut App) {
    match app.settings.save() {
        Ok(()) => {
            app.settings.saved_indicator = Some(Instant::now());
            app.settings.edit_buffer = None;
            app.input_mode = InputMode::Normal;
            app.set_status("配置已保存到 ~/.config/lan-share/config.toml".to_string());
        }
        Err(e) => {
            app.set_status(format!("保存失败: {}", e));
        }
    }
}

// ── UI rendering ────────────────────────────────────────────────────

fn ui(f: &mut Frame, app: &App) {
    let settings_width = if app.active_panel == ActivePanel::Settings {
        42u16
    } else {
        0u16
    };
    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(28),
            Constraint::Min(50),
            Constraint::Length(settings_width),
        ])
        .split(f.area());

    render_icon_bar(f, app, main_layout[0]);
    render_peer_list(f, app, main_layout[1]);
    render_chat_area(f, app, main_layout[2]);

    if app.active_panel == ActivePanel::Settings {
        render_settings_panel(f, app, main_layout[3]);
    }

    if app.show_file_picker {
        render_file_picker_popup(f, app);
    }

    if let Some((msg, _)) = &app.status_message {
        render_status_message(f, msg);
    }
}

fn render_icon_bar(f: &mut Frame, app: &App, area: Rect) {
    let rail_style = Style::default().bg(SERVER_RAIL);
    let server_style = Style::default().fg(Color::White).bg(BRAND).bold();
    let active_style = Style::default().fg(Color::White).bg(CHANNEL_HOVER).bold();
    let idle_style = Style::default().fg(MUTED).bg(SERVER_RAIL);

    let chat_marker =
        if app.active_panel == ActivePanel::Chat || app.active_panel == ActivePanel::Peers {
            ">"
        } else {
            " "
        };
    let settings_marker = if app.active_panel == ActivePanel::Settings {
        ">"
    } else {
        " "
    };

    let items = vec![
        ListItem::new(Line::from(Span::styled(" LS ", server_style))),
        ListItem::new(Line::from(Span::styled(
            "----",
            Style::default().fg(BORDER).bg(SERVER_RAIL),
        ))),
        ListItem::new(Line::from(vec![
            Span::styled(
                chat_marker,
                Style::default().fg(Color::White).bg(SERVER_RAIL),
            ),
            Span::styled(
                " # ",
                if chat_marker == ">" {
                    active_style
                } else {
                    idle_style
                },
            ),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled(
                settings_marker,
                Style::default().fg(Color::White).bg(SERVER_RAIL),
            ),
            Span::styled(
                " @ ",
                if settings_marker == ">" {
                    active_style
                } else {
                    idle_style
                },
            ),
        ])),
    ];

    let icon_bar = List::new(items).block(Block::default().style(rail_style));
    f.render_widget(icon_bar, area);
}

fn render_peer_list(f: &mut Frame, app: &App, area: Rect) {
    let title = format!(" LAN Share ");

    let items: Vec<ListItem> = app
        .peers
        .iter()
        .enumerate()
        .map(|(i, peer)| {
            let is_selected = app.selected_peer == Some(i);
            let style = if is_selected {
                Style::default().fg(Color::White).bg(CHANNEL_HOVER).bold()
            } else {
                Style::default().fg(MUTED).bg(CHANNEL_BG)
            };
            let ip_info = peer
                .ips
                .first()
                .map(|ip| format!("{}:{}", ip, peer.port))
                .unwrap_or_default();
            let has_unread = app
                .messages
                .get(&peer.uuid)
                .map(|m| !m.is_empty())
                .unwrap_or(false);

            ListItem::new(vec![
                Line::from(vec![
                    Span::styled("# ", style),
                    Span::styled(&peer.name, style),
                    if has_unread && !is_selected {
                        Span::styled(
                            " *",
                            Style::default().fg(Color::White).bg(CHANNEL_BG).bold(),
                        )
                    } else {
                        Span::raw("")
                    },
                ]),
                Line::from(Span::styled(
                    format!("  {}", ip_info),
                    Style::default().fg(MUTED).bg(CHANNEL_BG),
                )),
                Line::from(""),
            ])
        })
        .collect();

    let list_items = if items.is_empty() {
        vec![
            ListItem::new(Line::from(Span::styled(
                " ONLINE - 0",
                Style::default().fg(MUTED).bg(CHANNEL_BG).bold(),
            ))),
            ListItem::new(Line::from("")),
            ListItem::new(Line::from(Span::styled(
                "  等待局域网设备上线",
                Style::default().fg(MUTED).bg(CHANNEL_BG),
            ))),
        ]
    } else {
        let mut with_header = vec![
            ListItem::new(Line::from(Span::styled(
                format!(" ONLINE - {}", app.peers.len()),
                Style::default().fg(MUTED).bg(CHANNEL_BG).bold(),
            ))),
            ListItem::new(Line::from("")),
        ];
        with_header.extend(items);
        with_header
    };

    let list = List::new(list_items).block(
        Block::default()
            .title(title)
            .title_style(Style::default().fg(Color::White).bg(CHANNEL_BG).bold())
            .borders(Borders::RIGHT)
            .border_style(Style::default().fg(BORDER))
            .style(Style::default().bg(CHANNEL_BG)),
    );
    f.render_widget(list, area);

    let user_area = Rect::new(area.x, area.bottom().saturating_sub(4), area.width, 4);
    let user = Paragraph::new(vec![
        Line::from(Span::styled(
            " YOU",
            Style::default().fg(MUTED).bg(Color::Rgb(35, 36, 40)).bold(),
        )),
        Line::from(vec![
            Span::styled(
                "  ● ",
                Style::default().fg(GREEN).bg(Color::Rgb(35, 36, 40)),
            ),
            Span::styled(
                &app.node_name,
                Style::default().fg(TEXT).bg(Color::Rgb(35, 36, 40)),
            ),
        ]),
    ])
    .block(Block::default().style(Style::default().bg(Color::Rgb(35, 36, 40))));
    f.render_widget(user, user_area);
}

fn render_chat_area(f: &mut Frame, app: &App, area: Rect) {
    let chat_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(5),
        ])
        .split(area);

    // Header
    let peer_name = app
        .get_selected_peer()
        .map(|p| p.name.clone())
        .unwrap_or_else(|| "选择设备开始聊天".to_string());
    let peer_info = app
        .get_selected_peer()
        .map(|p| {
            let ip = p.ips.first().cloned().unwrap_or_default();
            format!("{}:{}", ip, p.port)
        })
        .unwrap_or_default();

    let header = Paragraph::new(Line::from(vec![
        Span::styled("# ", Style::default().fg(MUTED).bg(CHAT_BG).bold()),
        Span::styled(
            &peer_name,
            Style::default().fg(Color::White).bg(CHAT_BG).bold(),
        ),
        Span::styled("  |  ", Style::default().fg(MUTED).bg(CHAT_BG)),
        Span::styled(&peer_info, Style::default().fg(MUTED).bg(CHAT_BG)),
    ]))
    .block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(BORDER))
            .style(Style::default().bg(CHAT_BG)),
    );
    f.render_widget(header, chat_layout[0]);

    // Messages
    let messages: Vec<ChatMessage> = if let Some(peer) = app.get_selected_peer() {
        app.messages.get(&peer.uuid).cloned().unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut lines: Vec<Line> = Vec::new();

    if !messages.is_empty() {
        let date = messages
            .first()
            .unwrap()
            .timestamp
            .format("%Y年%m月%d日")
            .to_string();
        lines.push(Line::from(Span::styled(
            format!("──── {} ────", date),
            Style::default().fg(MUTED).bg(CHAT_BG),
        )));
        lines.push(Line::from(""));
    }

    for msg in &messages {
        if msg.is_system {
            lines.push(Line::from(Span::styled(
                format!("  + {}", msg.text),
                Style::default().fg(BRAND).bg(CHAT_BG),
            )));
            lines.push(Line::from(""));
            continue;
        }

        let time_str = msg.timestamp.format("今天 %H:%M").to_string();
        let sender_color = if msg.is_self { BRAND } else { GREEN };
        let avatar = sender_initial(&msg.sender);

        lines.push(Line::from(vec![
            Span::styled(
                format!(" {}  ", avatar),
                Style::default().fg(Color::White).bg(sender_color).bold(),
            ),
            Span::styled(
                &msg.sender,
                Style::default().fg(sender_color).bg(CHAT_BG).bold(),
            ),
            Span::styled("  ", Style::default().bg(CHAT_BG)),
            Span::styled(time_str, Style::default().fg(MUTED).bg(CHAT_BG)),
        ]));

        if msg.is_file {
            let size_str = msg.file_size.map(format_bytes).unwrap_or_default();
            lines.push(Line::from(vec![
                Span::styled(
                    "    [file] ",
                    Style::default().fg(Color::White).bg(CHANNEL_BG).bold(),
                ),
                Span::styled(
                    msg.file_name.clone().unwrap_or_default(),
                    Style::default().fg(Color::Cyan).bg(CHANNEL_BG).underlined(),
                ),
                Span::styled(
                    format!(" · {}", size_str),
                    Style::default().fg(MUTED).bg(CHANNEL_BG),
                ),
            ]));
        } else if !msg.text.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("    {}", msg.text),
                Style::default().fg(TEXT).bg(CHAT_BG),
            )));
        }
        lines.push(Line::from(""));
    }

    if messages.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "  欢迎来到 LAN Share",
            Style::default().fg(Color::White).bg(CHAT_BG).bold(),
        )));
        lines.push(Line::from(Span::styled(
            "  选择一个在线设备后，按 i 输入消息，按 f 发送文件。",
            Style::default().fg(MUTED).bg(CHAT_BG),
        )));
    }

    let scroll_from_bottom = if app.should_stick_to_bottom {
        0
    } else {
        app.message_scroll_offset
    };
    let scroll_y = scroll_offset_for_view(lines.len(), chat_layout[1].height, scroll_from_bottom);
    let messages_widget = Paragraph::new(lines)
        .block(Block::default().style(Style::default().bg(CHAT_BG)))
        .wrap(ratatui::widgets::Wrap { trim: false })
        .scroll((scroll_y, 0));
    f.render_widget(messages_widget, chat_layout[1]);

    // Input area
    let (prompt, prompt_style) = match app.input_mode {
        InputMode::Normal => ("  Message ", Style::default().fg(MUTED).bg(CHAT_INPUT)),
        InputMode::Editing => ("  Message ", Style::default().fg(TEXT).bg(CHAT_INPUT)),
        InputMode::FilePicker => (
            "  File path ",
            Style::default().fg(Color::Yellow).bg(CHAT_INPUT),
        ),
        InputMode::SettingsEdit => ("  Edit ", Style::default().fg(Color::Cyan).bg(CHAT_INPUT)),
    };

    let display_buffer = if app.input_mode == InputMode::SettingsEdit {
        app.settings.edit_buffer.as_deref().unwrap_or("")
    } else {
        &app.input_buffer
    };

    let input_lines = vec![
        Line::from(vec![
            Span::styled(prompt, prompt_style),
            Span::styled(
                display_buffer,
                Style::default().fg(Color::White).bg(CHAT_INPUT),
            ),
            if app.input_mode != InputMode::Normal {
                Span::styled("▎", Style::default().fg(Color::White).bg(CHAT_INPUT))
            } else {
                Span::raw("")
            },
        ]),
        Line::from(Span::styled(
            match app.input_mode {
                InputMode::Normal => {
                    "  i 输入 | f 文件 | Enter 打开会话 | s 设置 | Tab 切换 | q 退出"
                }
                InputMode::Editing => "  Enter 发送 | Esc 取消",
                InputMode::FilePicker => "  Enter 确认 | Esc 取消 | 支持 ~ 路径",
                InputMode::SettingsEdit => "  Enter 确认 | Esc 取消 | Ctrl+S 保存配置",
            },
            Style::default().fg(MUTED).bg(CHAT_INPUT),
        )),
    ];

    let input_block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(CHAT_BG))
        .style(Style::default().bg(CHAT_INPUT));
    let input_widget = Paragraph::new(input_lines).block(input_block);
    f.render_widget(input_widget, chat_layout[2]);
}

fn render_settings_panel(f: &mut Frame, app: &App, area: Rect) {
    let labels = SettingsState::field_labels();
    let rows: Vec<Row> = labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let value = if app.settings.edit_buffer.is_some() && app.settings.selected_field == i {
                format!("{}▎", app.settings.edit_buffer.as_deref().unwrap_or(""))
            } else {
                app.settings.get_field_value(i)
            };
            let style = if app.settings.selected_field == i {
                Style::default().fg(Color::White).bg(CHANNEL_HOVER).bold()
            } else {
                Style::default().fg(TEXT).bg(CHANNEL_BG)
            };
            Row::new(vec![
                Cell::from(Span::styled(
                    *label,
                    Style::default().fg(MUTED).bg(CHANNEL_BG),
                )),
                Cell::from(Span::styled(value, style)),
            ])
        })
        .collect();

    let widths = [Constraint::Length(12), Constraint::Min(20)];

    let saved_text = if app.settings.saved_indicator.is_some() {
        " ✓ 已保存"
    } else {
        ""
    };
    let table = Table::new(rows, widths)
        .header(Row::new(vec![
            Cell::from(Span::styled(
                "配置项",
                Style::default().fg(Color::White).bold(),
            )),
            Cell::from(Span::styled(
                "当前值",
                Style::default().fg(Color::White).bold(),
            )),
        ]))
        .block(
            Block::default()
                .title(format!(" Settings · config.toml{} ", saved_text))
                .title_style(Style::default().fg(Color::White).bg(CHANNEL_BG).bold())
                .borders(Borders::LEFT)
                .border_style(Style::default().fg(BORDER))
                .style(Style::default().bg(CHANNEL_BG)),
        );

    f.render_widget(table, area);

    let help = Paragraph::new(Line::from(Span::styled(
        " ↑↓ 选择 | Enter 编辑 | Ctrl+S 保存",
        Style::default().fg(MUTED).bg(CHANNEL_BG),
    )))
    .block(Block::default().style(Style::default().bg(CHANNEL_BG)));
    let help_area = Rect::new(area.x, area.bottom().saturating_sub(2), area.width, 2);
    f.render_widget(help, help_area);
}

fn render_file_picker_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 20, f.area());
    f.render_widget(Clear, area);

    let popup = Block::default()
        .title(" Send file ")
        .title_style(Style::default().fg(Color::White).bg(SERVER_RAIL).bold())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BRAND))
        .style(Style::default().bg(SERVER_RAIL));

    let content = Paragraph::new(vec![
        Line::from(Span::styled(
            "输入文件路径:",
            Style::default().fg(MUTED).bg(SERVER_RAIL),
        )),
        Line::from(vec![
            Span::styled(
                &app.input_buffer,
                Style::default().fg(Color::White).bg(SERVER_RAIL),
            ),
            Span::styled("▎", Style::default().fg(Color::White).bg(SERVER_RAIL)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "支持 ~ 开头表示主目录",
            Style::default().fg(MUTED).bg(SERVER_RAIL),
        )),
        Line::from(Span::styled(
            "Enter 确认，Esc 取消",
            Style::default().fg(MUTED).bg(SERVER_RAIL),
        )),
    ])
    .block(popup);
    f.render_widget(content, area);
}

fn render_status_message(f: &mut Frame, msg: &str) {
    let area = Rect::new(
        f.area().x,
        f.area().bottom().saturating_sub(1),
        f.area().width,
        1,
    );
    let status = Paragraph::new(Line::from(Span::styled(
        format!("  {} ", msg),
        Style::default().fg(Color::White).bg(GREEN),
    )));
    f.render_widget(status, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(v) = s.parse::<u64>() {
        return Some(v);
    }
    let (num_part, suffix) = if s.ends_with("GB") || s.ends_with("Gb") || s.ends_with("gb") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") || s.ends_with("Mb") || s.ends_with("mb") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") || s.ends_with("Kb") || s.ends_with("kb") {
        (&s[..s.len() - 2], 1024u64)
    } else {
        return None;
    };
    num_part
        .trim()
        .parse::<f64>()
        .ok()
        .map(|v| (v * suffix as f64) as u64)
}

fn sender_initial(sender: &str) -> char {
    sender
        .chars()
        .find(|c| !c.is_whitespace())
        .map(|c| c.to_ascii_uppercase())
        .unwrap_or('?')
}

fn bottom_scroll_offset(total_lines: usize, viewport_height: u16) -> u16 {
    total_lines.saturating_sub(viewport_height as usize) as u16
}

fn scroll_offset_for_view(total_lines: usize, viewport_height: u16, from_bottom: u16) -> u16 {
    bottom_scroll_offset(total_lines, viewport_height).saturating_sub(from_bottom)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app() -> App {
        App::new(&AppConfig::default())
    }

    fn peer(id: &str, name: &str) -> Peer {
        Peer {
            uuid: id.to_string(),
            name: name.to_string(),
            port: 8080,
            ips: vec!["127.0.0.1".to_string()],
        }
    }

    #[test]
    fn down_selects_first_peer_when_none_is_selected() {
        let mut app = test_app();
        app.peers = vec![peer("peer-1", "alpha"), peer("peer-2", "beta")];
        app.active_panel = ActivePanel::Peers;

        handle_normal_mode(&mut app, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));

        assert_eq!(app.selected_peer, Some(0));
    }

    #[test]
    fn new_messages_mark_chat_to_stick_to_bottom_without_extreme_scroll() {
        let mut app = test_app();

        app.add_message(
            "peer-1",
            ChatMessage {
                sender: "alpha".to_string(),
                text: "hello".to_string(),
                timestamp: chrono::Local::now(),
                is_self: false,
                is_file: false,
                file_name: None,
                file_size: None,
                is_system: false,
            },
        );

        assert!(app.should_stick_to_bottom);
        assert_ne!(app.message_scroll_offset, u16::MAX);
    }

    #[test]
    fn bottom_scroll_offset_never_exceeds_renderable_content() {
        assert_eq!(bottom_scroll_offset(4, 10), 0);
        assert_eq!(bottom_scroll_offset(12, 10), 2);
    }
}

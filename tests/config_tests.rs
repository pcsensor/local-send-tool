use lan_share::client::CompressionMode;
use lan_share::config::{
    expand_tilde_path, resolve_send_settings, resolve_serve_settings, AppConfig, ConfigOverrides,
    EnvConfig,
};
use std::path::PathBuf;

#[test]
fn config_file_path_always_uses_dot_config_under_home() {
    let home = PathBuf::from("/home/example");

    assert_eq!(
        lan_share::config::config_file_path_from_home(&home),
        home.join(".config").join("lan-share").join("config.toml")
    );
}

#[test]
fn leading_tilde_paths_expand_to_home_directory() {
    let home = PathBuf::from("/Users/example");

    assert_eq!(
        expand_tilde_path("~/Downloads", &home),
        home.join("Downloads")
    );
    assert_eq!(
        expand_tilde_path("~/.config/lan-share", &home),
        home.join(".config").join("lan-share")
    );
    assert_eq!(
        expand_tilde_path("~\\Downloads", &home),
        home.join("Downloads")
    );
    assert_eq!(
        expand_tilde_path("relative/path", &home),
        PathBuf::from("relative/path")
    );
    assert_eq!(
        expand_tilde_path("/absolute/path", &home),
        PathBuf::from("/absolute/path")
    );
}

#[test]
fn config_resolution_uses_cli_then_env_then_file_then_defaults() {
    let home = PathBuf::from("/home/test");
    let file_config = AppConfig::from_toml_str(
        r#"
        [defaults]
        download_dir = "~/from-file"
        port = 9001
        name = "file-name"
        bind_ip = "192.168.1.10"
        retry = 2
        compress = "never"
        progress = true
        cancel_timeout = 11
        chunked = true
        chunk_size = 4096
        chunk_concurrency = 5
        concurrency = 6
        "#,
    )
    .unwrap();

    let env_config = EnvConfig::from_pairs_with_home(
        [
            ("LAN_SHARE_DIR", "~/from-env"),
            ("LAN_SHARE_PORT", "9002"),
            ("LAN_SHARE_NAME", "env-name"),
            ("LAN_SHARE_BIND_IP", "192.168.1.20"),
            ("LAN_SHARE_RETRY", "3"),
            ("LAN_SHARE_COMPRESS", "always"),
            ("LAN_SHARE_PROGRESS", "false"),
            ("LAN_SHARE_CANCEL_TIMEOUT", "12"),
            ("LAN_SHARE_CHUNKED", "false"),
            ("LAN_SHARE_CHUNK_SIZE", "8192"),
            ("LAN_SHARE_CHUNK_CONCURRENCY", "7"),
            ("LAN_SHARE_CONCURRENCY", "8"),
        ],
        Some(&home),
    )
    .unwrap();

    let serve = resolve_serve_settings(
        &home,
        ConfigOverrides {
            port: Some(9003),
            ..ConfigOverrides::default()
        },
        &env_config,
        &file_config,
    );
    assert_eq!(serve.download_dir, home.join("from-env"));
    assert_eq!(serve.port, 9003);
    assert_eq!(serve.name.as_deref(), Some("env-name"));
    assert_eq!(serve.bind_ip.as_deref(), Some("192.168.1.20"));

    let send = resolve_send_settings(
        ConfigOverrides {
            retry: Some(4),
            compress: Some(CompressionMode::Auto),
            progress: Some(true),
            cancel_timeout: Some(13),
            chunked: Some(true),
            chunk_size: Some(16_384),
            chunk_concurrency: Some(9),
            concurrency: Some(10),
            ..ConfigOverrides::default()
        },
        &env_config,
        &file_config,
    );
    assert_eq!(send.retry_attempts, 4);
    assert_eq!(send.compression, CompressionMode::Auto);
    assert!(send.progress);
    assert_eq!(send.cancel_timeout, 13);
    assert!(send.chunked);
    assert_eq!(send.chunk_size, 16_384);
    assert_eq!(send.chunk_concurrency, 9);
    assert_eq!(send.concurrency, 10);
    assert_eq!(send.name.as_deref(), Some("env-name"));
    assert_eq!(send.bind_ip.as_deref(), Some("192.168.1.20"));
}

#[test]
fn config_file_defaults_apply_when_cli_and_env_are_absent() {
    let file_config = AppConfig::from_toml_str(
        r#"
        [defaults]
        progress = true
        cancel_timeout = 21
        chunked = true
        chunk_size = 32768
        chunk_concurrency = 11
        concurrency = 12
        "#,
    )
    .unwrap();

    let send = resolve_send_settings(
        ConfigOverrides::default(),
        &EnvConfig::default(),
        &file_config,
    );

    assert!(send.progress);
    assert_eq!(send.cancel_timeout, 21);
    assert!(send.chunked);
    assert_eq!(send.chunk_size, 32_768);
    assert_eq!(send.chunk_concurrency, 11);
    assert_eq!(send.concurrency, 12);
}

#[test]
fn cli_download_dir_expands_leading_tilde() {
    let home = PathBuf::from("/home/test");
    let settings = resolve_serve_settings(
        &home,
        ConfigOverrides {
            download_dir: Some(PathBuf::from("~/Downloads")),
            ..ConfigOverrides::default()
        },
        &EnvConfig::default(),
        &AppConfig::default(),
    );

    assert_eq!(settings.download_dir, home.join("Downloads"));
}

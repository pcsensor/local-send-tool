use lan_share::client::CompressionMode;
use lan_share::config::{
    resolve_send_settings, resolve_serve_settings, AppConfig, ConfigOverrides, EnvConfig,
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
fn config_resolution_uses_cli_then_env_then_file_then_defaults() {
    let file_config = AppConfig::from_toml_str(
        r#"
        [defaults]
        download_dir = "/from-file"
        port = 9001
        name = "file-name"
        bind_ip = "192.168.1.10"
        retry = 2
        compress = "never"
        "#,
    )
    .unwrap();

    let env_config = EnvConfig::from_pairs([
        ("LAN_SHARE_DIR", "/from-env"),
        ("LAN_SHARE_PORT", "9002"),
        ("LAN_SHARE_NAME", "env-name"),
        ("LAN_SHARE_BIND_IP", "192.168.1.20"),
        ("LAN_SHARE_RETRY", "3"),
        ("LAN_SHARE_COMPRESS", "always"),
    ])
    .unwrap();

    let serve = resolve_serve_settings(
        ConfigOverrides {
            port: Some(9003),
            ..ConfigOverrides::default()
        },
        &env_config,
        &file_config,
    );
    assert_eq!(serve.download_dir, PathBuf::from("/from-env"));
    assert_eq!(serve.port, 9003);
    assert_eq!(serve.name.as_deref(), Some("env-name"));
    assert_eq!(serve.bind_ip.as_deref(), Some("192.168.1.20"));

    let send = resolve_send_settings(
        ConfigOverrides {
            retry: Some(4),
            compress: Some(CompressionMode::Auto),
            ..ConfigOverrides::default()
        },
        &env_config,
        &file_config,
    );
    assert_eq!(send.retry_attempts, 4);
    assert_eq!(send.compression, CompressionMode::Auto);
    assert_eq!(send.name.as_deref(), Some("env-name"));
    assert_eq!(send.bind_ip.as_deref(), Some("192.168.1.20"));
}

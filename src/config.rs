use crate::client::CompressionMode;
use directories::UserDirs;
use serde::Deserialize;
use std::{
    collections::HashMap,
    env,
    error::Error,
    path::{Path, PathBuf},
};

type ConfigResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub defaults: ConfigDefaults,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ConfigDefaults {
    pub download_dir: Option<PathBuf>,
    pub port: Option<u16>,
    pub name: Option<String>,
    pub bind_ip: Option<String>,
    pub retry: Option<usize>,
    pub compress: Option<CompressionMode>,
}

#[derive(Clone, Debug, Default)]
pub struct EnvConfig {
    pub download_dir: Option<PathBuf>,
    pub port: Option<u16>,
    pub name: Option<String>,
    pub bind_ip: Option<String>,
    pub retry: Option<usize>,
    pub compress: Option<CompressionMode>,
}

#[derive(Clone, Debug, Default)]
pub struct ConfigOverrides {
    pub download_dir: Option<PathBuf>,
    pub port: Option<u16>,
    pub name: Option<String>,
    pub bind_ip: Option<String>,
    pub retry: Option<usize>,
    pub compress: Option<CompressionMode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeSettings {
    pub download_dir: PathBuf,
    pub port: u16,
    pub name: Option<String>,
    pub bind_ip: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SendSettings {
    pub name: Option<String>,
    pub bind_ip: Option<String>,
    pub retry_attempts: usize,
    pub compression: CompressionMode,
}

impl AppConfig {
    pub fn load() -> ConfigResult<Self> {
        let Some(path) = config_file_path() else {
            return Ok(Self::default());
        };
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)?;
        Self::from_toml_str(&content)
    }

    pub fn from_toml_str(content: &str) -> ConfigResult<Self> {
        Ok(toml::from_str(content)?)
    }
}

impl EnvConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Self::from_pairs(env::vars())
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> ConfigResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values: HashMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();

        Ok(Self {
            download_dir: values.get("LAN_SHARE_DIR").map(PathBuf::from),
            port: parse_optional(values.get("LAN_SHARE_PORT"), "LAN_SHARE_PORT")?,
            name: values.get("LAN_SHARE_NAME").cloned(),
            bind_ip: values.get("LAN_SHARE_BIND_IP").cloned(),
            retry: parse_optional(values.get("LAN_SHARE_RETRY"), "LAN_SHARE_RETRY")?,
            compress: match values.get("LAN_SHARE_COMPRESS") {
                Some(value) => Some(parse_compression_mode(value)?),
                None => None,
            },
        })
    }
}

pub fn config_file_path() -> Option<PathBuf> {
    UserDirs::new().map(|dirs| config_file_path_from_home(dirs.home_dir()))
}

pub fn config_file_path_from_home(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref()
        .join(".config")
        .join("lan-share")
        .join("config.toml")
}

pub fn resolve_serve_settings(
    cli: ConfigOverrides,
    env: &EnvConfig,
    config: &AppConfig,
) -> ServeSettings {
    ServeSettings {
        download_dir: cli
            .download_dir
            .or_else(|| env.download_dir.clone())
            .or_else(|| config.defaults.download_dir.clone())
            .unwrap_or_else(|| PathBuf::from("./downloads")),
        port: cli
            .port
            .or(env.port)
            .or(config.defaults.port)
            .unwrap_or(8080),
        name: cli
            .name
            .or_else(|| env.name.clone())
            .or_else(|| config.defaults.name.clone()),
        bind_ip: cli
            .bind_ip
            .or_else(|| env.bind_ip.clone())
            .or_else(|| config.defaults.bind_ip.clone()),
    }
}

pub fn resolve_send_settings(
    cli: ConfigOverrides,
    env: &EnvConfig,
    config: &AppConfig,
) -> SendSettings {
    SendSettings {
        name: cli
            .name
            .or_else(|| env.name.clone())
            .or_else(|| config.defaults.name.clone()),
        bind_ip: cli
            .bind_ip
            .or_else(|| env.bind_ip.clone())
            .or_else(|| config.defaults.bind_ip.clone()),
        retry_attempts: cli
            .retry
            .or(env.retry)
            .or(config.defaults.retry)
            .unwrap_or(0),
        compression: cli
            .compress
            .or(env.compress)
            .or(config.defaults.compress)
            .unwrap_or_default(),
    }
}

fn parse_optional<T>(value: Option<&String>, name: &str) -> ConfigResult<Option<T>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    value
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|err| format!("Invalid {} value '{}': {}", name, value, err).into())
        })
        .transpose()
}

fn parse_compression_mode(value: &str) -> ConfigResult<CompressionMode> {
    match value {
        "auto" => Ok(CompressionMode::Auto),
        "always" => Ok(CompressionMode::Always),
        "never" => Ok(CompressionMode::Never),
        other => Err(format!(
            "Invalid LAN_SHARE_COMPRESS value '{}'; expected auto, always, or never",
            other
        )
        .into()),
    }
}

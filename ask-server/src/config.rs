use anyhow::{Context, Result};

pub const SQLITE_PATH_ENV: &str = "ASK_SERVER_SQLITE_PATH";
pub const BIND_HOST_ENV: &str = "ASK_SERVER_BIND_HOST";
pub const BIND_PORT_ENV: &str = "ASK_SERVER_BIND_PORT";

pub const DEFAULT_SQLITE_PATH: &str = "data/ask.sqlite3";
pub const DEFAULT_BIND_HOST: &str = "0.0.0.0";
pub const DEFAULT_BIND_PORT: u16 = 3000;

/// Server configuration loaded from environment variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Filesystem path to the SQLite database file.
    pub sqlite_path: String,
    /// Host or interface the HTTP server binds to.
    pub bind_host: String,
    /// TCP port the HTTP server binds to.
    pub bind_port: u16,
}

impl Config {
    /// Returns the socket address string used by the HTTP listener.
    #[must_use]
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.bind_host, self.bind_port)
    }
}

/// Loads server configuration from environment variables.
///
/// # Errors
///
/// Returns an error if `ASK_SERVER_BIND_PORT` is set but cannot be parsed as a
/// valid `u16` TCP port.
pub fn load() -> Result<Config> {
    let sqlite_path =
        std::env::var(SQLITE_PATH_ENV).unwrap_or_else(|_| String::from(DEFAULT_SQLITE_PATH));
    let bind_host =
        std::env::var(BIND_HOST_ENV).unwrap_or_else(|_| String::from(DEFAULT_BIND_HOST));
    let bind_port = match std::env::var(BIND_PORT_ENV) {
        Ok(raw_port) => raw_port.parse::<u16>().with_context(|| {
            format!(
                "failed to parse {} value '{}' as a valid TCP port",
                BIND_PORT_ENV, raw_port
            )
        })?,
        Err(_) => DEFAULT_BIND_PORT,
    };

    Ok(Config {
        sqlite_path,
        bind_host,
        bind_port,
    })
}

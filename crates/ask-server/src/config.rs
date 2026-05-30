use anyhow::{Context, Result, anyhow, bail};

pub const DATA_DIR_ENV: &str = "ASK_SERVER_DATA_DIR";
pub const RESOURCE_DIR_ENV: &str = "ASK_SERVER_RESOURCE_DIR";
pub const BIND_HOST_ENV: &str = "ASK_SERVER_BIND_HOST";
pub const BIND_PORT_ENV: &str = "ASK_SERVER_BIND_PORT";
pub const EMBEDDING_MODE_ENV: &str = "ASK_SERVER_EMBEDDING_MODE";
pub const EMBEDDING_BASE_URL_ENV: &str = "ASK_SERVER_EMBEDDING_BASE_URL";
pub const EMBEDDING_AUTH_TOKEN_ENV: &str = "ASK_SERVER_EMBEDDING_AUTH_TOKEN";

pub const DEFAULT_DATA_DIR: &str = ".data";
pub const DEFAULT_RESOURCE_DIR: &str = ".";
pub const DEFAULT_BIND_HOST: &str = "0.0.0.0";
pub const DEFAULT_BIND_PORT: u16 = 3000;
pub const DEFAULT_TEI_BASE_URL: &str = "http://localhost:18080";

/// Embedding backend configuration loaded from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingProvider {
    /// Use the local text-embeddings-inference service.
    Tei {
        /// OpenAI-compatible TEI base URL.
        base_url: String,
    },
    /// Use an external OpenAI-compatible embeddings provider.
    OpenAi {
        /// Provider base URL.
        base_url: String,
        /// Bearer token used for authenticated requests.
        auth_token: String,
    },
}

impl EmbeddingProvider {
    /// Returns the configured provider base URL.
    #[must_use]
    pub fn base_url(&self) -> &str {
        match self {
            Self::Tei { base_url } | Self::OpenAi { base_url, .. } => base_url,
        }
    }

    /// Returns a stable mode label for logs and diagnostics.
    #[must_use]
    pub fn mode_name(&self) -> &'static str {
        match self {
            Self::Tei { .. } => "tei",
            Self::OpenAi { .. } => "openai",
        }
    }
}

/// Server configuration loaded from environment variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Filesystem path to the directory containing persistent server data.
    pub data_dir: String,
    /// Filesystem path to the directory containing resource files (code, configs, etc.).
    pub resource_dir: String,
    /// Host or interface the HTTP server binds to.
    pub bind_host: String,
    /// TCP port the HTTP server binds to.
    pub bind_port: u16,
    /// Embedding provider configuration.
    pub embedding_provider: EmbeddingProvider,
}

impl Config {
    /// Returns the socket address string used by the HTTP listener.
    #[must_use]
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.bind_host, self.bind_port)
    }

    /// Returns the SQLite path derived from the configured data directory.
    #[must_use]
    pub fn sqlite_path(&self) -> String {
        format!(
            "{}/ask.sqlite3",
            self.data_dir.trim_end_matches(&['/', '\\'][..])
        )
    }
}

/// Loads server configuration from environment variables.
///
/// # Errors
///
/// Returns an error if a numeric field is invalid or an embedding mode is
/// missing required configuration.
pub fn load() -> Result<Config> {
    let data_dir = std::env::var(DATA_DIR_ENV).unwrap_or_else(|_| String::from(DEFAULT_DATA_DIR));
    let resource_dir =
        std::env::var(RESOURCE_DIR_ENV).unwrap_or_else(|_| String::from(DEFAULT_RESOURCE_DIR));
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
    let embedding_provider = load_embedding_provider()?;

    Ok(Config {
        data_dir,
        resource_dir,
        bind_host,
        bind_port,
        embedding_provider,
    })
}

fn load_embedding_provider() -> Result<EmbeddingProvider> {
    let mode = std::env::var(EMBEDDING_MODE_ENV).unwrap_or_else(|_| String::from("tei"));

    match mode.as_str() {
        "tei" => {
            let base_url = std::env::var(EMBEDDING_BASE_URL_ENV)
                .unwrap_or_else(|_| String::from(DEFAULT_TEI_BASE_URL));
            Ok(EmbeddingProvider::Tei { base_url })
        }
        "openai" => {
            let base_url = std::env::var(EMBEDDING_BASE_URL_ENV).map_err(|_| {
                anyhow!(
                    "{} must be set when {}=openai",
                    EMBEDDING_BASE_URL_ENV,
                    EMBEDDING_MODE_ENV
                )
            })?;
            let auth_token = std::env::var(EMBEDDING_AUTH_TOKEN_ENV).map_err(|_| {
                anyhow!(
                    "{} must be set when {}=openai",
                    EMBEDDING_AUTH_TOKEN_ENV,
                    EMBEDDING_MODE_ENV
                )
            })?;

            if auth_token.trim().is_empty() {
                bail!(
                    "{} must not be empty when {}=openai",
                    EMBEDDING_AUTH_TOKEN_ENV,
                    EMBEDDING_MODE_ENV
                );
            }

            Ok(EmbeddingProvider::OpenAi {
                base_url,
                auth_token,
            })
        }
        _ => bail!(
            "{} must be either 'tei' or 'openai', got '{}'",
            EMBEDDING_MODE_ENV,
            mode
        ),
    }
}

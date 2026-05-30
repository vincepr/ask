use anyhow::{Context, Result, anyhow, bail, ensure};

pub const DATA_DIR_ENV: &str = "ASK_SERVER_DATA_DIR";
pub const RESOURCE_DIR_ENV: &str = "ASK_SERVER_RESOURCE_DIR";
pub const BIND_HOST_ENV: &str = "ASK_SERVER_BIND_HOST";
pub const BIND_PORT_ENV: &str = "ASK_SERVER_BIND_PORT";
pub const EMBEDDING_MODEL_ENV: &str = "ASK_SERVER_EMBEDDING_MODEL";
pub const EMBEDDING_DIMENSIONS_ENV: &str = "ASK_SERVER_EMBEDDING_DIMENSIONS";
pub const EMBEDDING_CHUNK_SIZE_ENV: &str = "ASK_SERVER_EMBEDDING_CHUNK_SIZE";
pub const EMBEDDING_CHUNK_OVERLAP_ENV: &str = "ASK_SERVER_EMBEDDING_CHUNK_OVERLAP";
pub const EMBEDDING_MODE_ENV: &str = "ASK_SERVER_EMBEDDING_MODE";
pub const EMBEDDING_BASE_URL_ENV: &str = "ASK_SERVER_EMBEDDING_BASE_URL";
pub const EMBEDDING_AUTH_TOKEN_ENV: &str = "ASK_SERVER_EMBEDDING_AUTH_TOKEN";

pub const DEFAULT_DATA_DIR: &str = ".data";
pub const DEFAULT_RESOURCE_DIR: &str = ".";
pub const DEFAULT_BIND_HOST: &str = "0.0.0.0";
pub const DEFAULT_BIND_PORT: u16 = 3000;
pub const DEFAULT_EMBEDDING_MODEL: &str = "default";
pub const DEFAULT_EMBEDDING_DIMENSIONS: i64 = 768;
pub const DEFAULT_EMBEDDING_CHUNK_SIZE: i64 = 512;
pub const DEFAULT_EMBEDDING_CHUNK_OVERLAP: i64 = 0;
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
    /// Embedding model name.
    pub embedding_model: String,
    /// Embedding vector dimensions.
    pub embedding_dimensions: i64,
    /// Max tokens per chunk for this model.
    pub embedding_chunk_size: i64,
    /// Token overlap between consecutive chunks.
    pub embedding_chunk_overlap: i64,
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
    let embedding_model = std::env::var(EMBEDDING_MODEL_ENV)
        .unwrap_or_else(|_| String::from(DEFAULT_EMBEDDING_MODEL));
    let embedding_dimensions = match std::env::var(EMBEDDING_DIMENSIONS_ENV) {
        Ok(raw) => raw.parse::<i64>().with_context(|| {
            format!(
                "failed to parse {} value '{}' as an integer",
                EMBEDDING_DIMENSIONS_ENV, raw
            )
        })?,
        Err(_) => DEFAULT_EMBEDDING_DIMENSIONS,
    };
    let embedding_chunk_size = match std::env::var(EMBEDDING_CHUNK_SIZE_ENV) {
        Ok(raw) => raw.parse::<i64>().with_context(|| {
            format!(
                "failed to parse {} value '{}' as an integer",
                EMBEDDING_CHUNK_SIZE_ENV, raw
            )
        })?,
        Err(_) => DEFAULT_EMBEDDING_CHUNK_SIZE,
    };
    let embedding_chunk_overlap = match std::env::var(EMBEDDING_CHUNK_OVERLAP_ENV) {
        Ok(raw) => raw.parse::<i64>().with_context(|| {
            format!(
                "failed to parse {} value '{}' as an integer",
                EMBEDDING_CHUNK_OVERLAP_ENV, raw
            )
        })?,
        Err(_) => DEFAULT_EMBEDDING_CHUNK_OVERLAP,
    };
    let embedding_provider = load_embedding_provider()?;
    validate_embedding_settings(
        embedding_dimensions,
        embedding_chunk_size,
        embedding_chunk_overlap,
    )?;

    Ok(Config {
        data_dir,
        resource_dir,
        bind_host,
        bind_port,
        embedding_model,
        embedding_dimensions,
        embedding_chunk_size,
        embedding_chunk_overlap,
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

fn validate_embedding_settings(
    embedding_dimensions: i64,
    embedding_chunk_size: i64,
    embedding_chunk_overlap: i64,
) -> Result<()> {
    ensure!(
        embedding_dimensions > 0,
        "{} must be greater than 0, got {}",
        EMBEDDING_DIMENSIONS_ENV,
        embedding_dimensions
    );
    ensure!(
        embedding_chunk_size > 0,
        "{} must be greater than 0, got {}",
        EMBEDDING_CHUNK_SIZE_ENV,
        embedding_chunk_size
    );
    ensure!(
        embedding_chunk_overlap >= 0,
        "{} must be greater than or equal to 0, got {}",
        EMBEDDING_CHUNK_OVERLAP_ENV,
        embedding_chunk_overlap
    );
    ensure!(
        embedding_chunk_overlap < embedding_chunk_size,
        "{} must be less than {}, got {} >= {}",
        EMBEDDING_CHUNK_OVERLAP_ENV,
        EMBEDDING_CHUNK_SIZE_ENV,
        embedding_chunk_overlap,
        embedding_chunk_size
    );

    Ok(())
}

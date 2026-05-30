use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ask_server::{
    config::{
        BIND_HOST_ENV, BIND_PORT_ENV, DATA_DIR_ENV, DEFAULT_BIND_HOST, DEFAULT_BIND_PORT,
        DEFAULT_DATA_DIR, DEFAULT_TEI_BASE_URL, EMBEDDING_AUTH_TOKEN_ENV, EMBEDDING_BASE_URL_ENV,
        EMBEDDING_MODE_ENV, EmbeddingProvider, load,
    },
    open_database,
};

#[test]
fn load_uses_defaults_when_env_is_missing() {
    let _data_dir_guard = EnvVarGuard::unset(DATA_DIR_ENV);
    let _host_guard = EnvVarGuard::unset(BIND_HOST_ENV);
    let _port_guard = EnvVarGuard::unset(BIND_PORT_ENV);
    let _mode_guard = EnvVarGuard::unset(EMBEDDING_MODE_ENV);
    let _base_url_guard = EnvVarGuard::unset(EMBEDDING_BASE_URL_ENV);
    let _token_guard = EnvVarGuard::unset(EMBEDDING_AUTH_TOKEN_ENV);

    let config = load().expect("config load must succeed");

    assert_eq!(config.data_dir, DEFAULT_DATA_DIR);
    assert_eq!(config.bind_host, DEFAULT_BIND_HOST);
    assert_eq!(config.bind_port, DEFAULT_BIND_PORT);
    assert_eq!(config.bind_address(), "0.0.0.0:3000");
    assert_eq!(config.sqlite_path(), ".data/ask.sqlite3");
    assert_eq!(
        config.embedding_provider,
        EmbeddingProvider::Tei {
            base_url: String::from(DEFAULT_TEI_BASE_URL),
        }
    );
}

#[test]
fn load_uses_env_overrides_when_present() {
    let _data_dir_guard = EnvVarGuard::set(DATA_DIR_ENV, "custom-data");
    let _host_guard = EnvVarGuard::set(BIND_HOST_ENV, "127.0.0.1");
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "4123");
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "tei");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "http://127.0.0.1:18080");

    let config = load().expect("config load must succeed");

    assert_eq!(config.data_dir, "custom-data");
    assert_eq!(config.bind_host, "127.0.0.1");
    assert_eq!(config.bind_port, 4123);
    assert_eq!(config.bind_address(), "127.0.0.1:4123");
    assert_eq!(config.sqlite_path(), "custom-data/ask.sqlite3");
    assert_eq!(
        config.embedding_provider,
        EmbeddingProvider::Tei {
            base_url: String::from("http://127.0.0.1:18080"),
        }
    );
}

#[test]
fn load_rejects_invalid_bind_port() {
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "invalid-port");

    let error = load().expect_err("config load must fail for invalid port");

    assert!(error.to_string().contains(BIND_PORT_ENV));
}

#[test]
fn load_requires_openai_credentials_in_openai_mode() {
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "openai");
    let _base_url_guard = EnvVarGuard::unset(EMBEDDING_BASE_URL_ENV);
    let _token_guard = EnvVarGuard::unset(EMBEDDING_AUTH_TOKEN_ENV);

    let error = load().expect_err("config load must fail without openai credentials");

    assert!(error.to_string().contains(EMBEDDING_BASE_URL_ENV));
}

#[test]
fn load_uses_openai_provider_when_fully_configured() {
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "openai");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "https://api.openai.example/v1");
    let _token_guard = EnvVarGuard::set(EMBEDDING_AUTH_TOKEN_ENV, "secret-token");

    let config = load().expect("config load must succeed");

    assert_eq!(
        config.embedding_provider,
        EmbeddingProvider::OpenAi {
            base_url: String::from("https://api.openai.example/v1"),
            auth_token: String::from("secret-token"),
        }
    );
}

#[test]
fn load_rejects_unknown_embedding_mode() {
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "unknown");

    let error = load().expect_err("config load must fail for unknown embedding mode");

    assert!(error.to_string().contains(EMBEDDING_MODE_ENV));
}

#[test]
fn open_database_creates_parent_directory() {
    let temp_dir = unique_temp_dir();
    let database_path = temp_dir.join("nested").join("ask.sqlite3");

    let connection = open_database(&database_path.to_string_lossy())
        .expect("database open must create parent directory");

    drop(connection);

    assert!(database_path.exists());

    std::fs::remove_dir_all(&temp_dir).expect("temporary test directory must be removable");
}

struct EnvVarGuard {
    key: &'static str,
    original_value: Option<String>,
}

impl EnvVarGuard {
    fn unset(key: &'static str) -> Self {
        let original_value = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }

        Self {
            key,
            original_value,
        }
    }

    fn set(key: &'static str, value: &str) -> Self {
        let original_value = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }

        Self {
            key,
            original_value,
        }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.original_value {
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

fn unique_temp_dir() -> PathBuf {
    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after unix epoch")
        .as_nanos();

    std::env::temp_dir().join(format!("ask-server-tests-{unique_suffix}"))
}

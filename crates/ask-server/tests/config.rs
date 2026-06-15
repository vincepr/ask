use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ask_server::{
    config::{
        BIND_HOST_ENV, BIND_PORT_ENV, DATA_DIR_ENV, DATA_DISPLAY_DIR_ENV, DATABASE_POOL_SIZE_ENV,
        DEFAULT_BIND_HOST, DEFAULT_BIND_PORT, DEFAULT_DATA_DIR, DEFAULT_DATABASE_POOL_SIZE,
        DEFAULT_EMBEDDING_MAX_BATCH_SIZE, DEFAULT_TEI_BASE_URL, DEFAULT_WORKER_COUNT,
        EMBEDDING_AUTH_TOKEN_ENV, EMBEDDING_BASE_URL_ENV, EMBEDDING_CHUNK_OVERLAP_ENV,
        EMBEDDING_CHUNK_SIZE_ENV, EMBEDDING_DIMENSIONS_ENV, EMBEDDING_MAX_BATCH_SIZE_ENV,
        EMBEDDING_MODE_ENV, EMBEDDING_MODEL_ENV, EMBEDDING_WORKER_COUNT_ENV, EmbeddingProvider,
        RESOURCE_DISPLAY_DIR_ENV, load,
    },
    create_pool_with_max_size, open_database,
};

#[test]
fn load_uses_defaults_when_optional_env_is_missing() {
    let _env_lock = env_lock();
    let _data_dir_guard = EnvVarGuard::unset(DATA_DIR_ENV);
    let _data_display_dir_guard = EnvVarGuard::unset(DATA_DISPLAY_DIR_ENV);
    let _resource_display_dir_guard = EnvVarGuard::unset(RESOURCE_DISPLAY_DIR_ENV);
    let _host_guard = EnvVarGuard::unset(BIND_HOST_ENV);
    let _port_guard = EnvVarGuard::unset(BIND_PORT_ENV);
    let _mode_guard = EnvVarGuard::unset(EMBEDDING_MODE_ENV);
    let _base_url_guard = EnvVarGuard::unset(EMBEDDING_BASE_URL_ENV);
    let _token_guard = EnvVarGuard::unset(EMBEDDING_AUTH_TOKEN_ENV);
    let _pool_guard = EnvVarGuard::unset(DATABASE_POOL_SIZE_ENV);
    let _model_guard = EnvVarGuard::set(
        EMBEDDING_MODEL_ENV,
        "onnx-community/Qwen3-Embedding-0.6B-ONNX",
    );

    let config = load().expect("config load must succeed");

    assert_eq!(config.data_dir, DEFAULT_DATA_DIR);
    assert_eq!(config.data_display_dir, DEFAULT_DATA_DIR);
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
    assert_eq!(
        config.embedding_model,
        "onnx-community/Qwen3-Embedding-0.6B-ONNX"
    );
    assert_eq!(
        config.embedding_max_batch_size,
        DEFAULT_EMBEDDING_MAX_BATCH_SIZE
    );
    assert_eq!(config.embedding_worker_count, DEFAULT_WORKER_COUNT);
    assert_eq!(config.database_pool_size, DEFAULT_DATABASE_POOL_SIZE);
    assert!(config.database_pool_size > DEFAULT_WORKER_COUNT);
}

#[test]
fn load_uses_env_overrides_when_present() {
    let _env_lock = env_lock();
    let _data_dir_guard = EnvVarGuard::set(DATA_DIR_ENV, "custom-data");
    let _data_display_dir_guard = EnvVarGuard::unset(DATA_DISPLAY_DIR_ENV);
    let _resource_display_dir_guard = EnvVarGuard::unset(RESOURCE_DISPLAY_DIR_ENV);
    let _host_guard = EnvVarGuard::set(BIND_HOST_ENV, "127.0.0.1");
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "4123");
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "tei");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "http://127.0.0.1:18080");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");
    let _batch_guard = EnvVarGuard::set(EMBEDDING_MAX_BATCH_SIZE_ENV, "64");
    let _worker_guard = EnvVarGuard::set(EMBEDDING_WORKER_COUNT_ENV, "4");
    let _pool_guard = EnvVarGuard::set(DATABASE_POOL_SIZE_ENV, "9");

    let config = load().expect("config load must succeed");

    assert_eq!(config.data_dir, "custom-data");
    assert_eq!(config.data_display_dir, "custom-data");
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
    assert_eq!(config.embedding_model, "custom-model");
    assert_eq!(config.embedding_max_batch_size, 64);
    assert_eq!(config.embedding_worker_count, 4);
    assert_eq!(config.database_pool_size, 9);
}

#[test]
fn load_uses_internal_display_dir_overrides_when_present() {
    let _env_lock = env_lock();
    let _data_dir_guard = EnvVarGuard::set(DATA_DIR_ENV, "/data");
    let _data_display_dir_guard = EnvVarGuard::set(DATA_DISPLAY_DIR_ENV, ".data");
    let _resource_display_dir_guard = EnvVarGuard::set(RESOURCE_DISPLAY_DIR_ENV, ".");
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "tei");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "http://127.0.0.1:18080");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let config = load().expect("config load must succeed");

    assert_eq!(config.data_dir, "/data");
    assert_eq!(config.data_display_dir, ".data");
    assert_eq!(config.resource_display_dir, ".");
}

#[test]
fn load_rejects_invalid_bind_port() {
    let _env_lock = env_lock();
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "invalid-port");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for invalid port");

    assert!(error.to_string().contains(BIND_PORT_ENV));
}

#[test]
fn load_requires_openai_credentials_in_openai_mode() {
    let _env_lock = env_lock();
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "openai");
    let _base_url_guard = EnvVarGuard::unset(EMBEDDING_BASE_URL_ENV);
    let _token_guard = EnvVarGuard::unset(EMBEDDING_AUTH_TOKEN_ENV);
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "text-embedding-3-small");

    let error = load().expect_err("config load must fail without openai credentials");

    assert!(error.to_string().contains(EMBEDDING_BASE_URL_ENV));
}

#[test]
fn load_uses_openai_provider_when_fully_configured() {
    let _env_lock = env_lock();
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "openai");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "https://api.openai.example/v1");
    let _token_guard = EnvVarGuard::set(EMBEDDING_AUTH_TOKEN_ENV, "secret-token");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "text-embedding-3-small");

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
fn load_rejects_empty_tei_base_url() {
    let _env_lock = env_lock();
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "tei");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "   ");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for empty tei base url");

    assert!(error.to_string().contains(EMBEDDING_BASE_URL_ENV));
}

#[test]
fn load_rejects_empty_openai_base_url() {
    let _env_lock = env_lock();
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "openai");
    let _base_url_guard = EnvVarGuard::set(EMBEDDING_BASE_URL_ENV, "   ");
    let _token_guard = EnvVarGuard::set(EMBEDDING_AUTH_TOKEN_ENV, "secret-token");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "text-embedding-3-small");

    let error = load().expect_err("config load must fail for empty openai base url");

    assert!(error.to_string().contains(EMBEDDING_BASE_URL_ENV));
}

#[test]
fn load_rejects_unknown_embedding_mode() {
    let _env_lock = env_lock();
    let _mode_guard = EnvVarGuard::set(EMBEDDING_MODE_ENV, "unknown");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for unknown embedding mode");

    assert!(error.to_string().contains(EMBEDDING_MODE_ENV));
}

#[test]
fn load_rejects_non_positive_embedding_dimensions() {
    let _env_lock = env_lock();
    let _dimensions_guard = EnvVarGuard::set(EMBEDDING_DIMENSIONS_ENV, "0");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for non-positive dimensions");

    assert!(error.to_string().contains(EMBEDDING_DIMENSIONS_ENV));
}

#[test]
fn load_rejects_non_positive_embedding_chunk_size() {
    let _env_lock = env_lock();
    let _chunk_size_guard = EnvVarGuard::set(EMBEDDING_CHUNK_SIZE_ENV, "-1");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for non-positive chunk size");

    assert!(error.to_string().contains(EMBEDDING_CHUNK_SIZE_ENV));
}

#[test]
fn load_rejects_negative_embedding_chunk_overlap() {
    let _env_lock = env_lock();
    let _chunk_overlap_guard = EnvVarGuard::set(EMBEDDING_CHUNK_OVERLAP_ENV, "-1");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail for negative chunk overlap");

    assert!(error.to_string().contains(EMBEDDING_CHUNK_OVERLAP_ENV));
}

#[test]
fn load_rejects_embedding_chunk_overlap_equal_to_chunk_size() {
    let _env_lock = env_lock();
    let _chunk_size_guard = EnvVarGuard::set(EMBEDDING_CHUNK_SIZE_ENV, "32");
    let _chunk_overlap_guard = EnvVarGuard::set(EMBEDDING_CHUNK_OVERLAP_ENV, "32");
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");

    let error = load().expect_err("config load must fail when overlap matches chunk size");

    assert!(error.to_string().contains(EMBEDDING_CHUNK_OVERLAP_ENV));
}

#[test]
fn load_requires_embedding_model() {
    let _env_lock = env_lock();
    let _model_guard = EnvVarGuard::unset(EMBEDDING_MODEL_ENV);

    let error = load().expect_err("config load must fail without embedding model");

    assert!(error.to_string().contains(EMBEDDING_MODEL_ENV));
}

#[test]
fn load_rejects_empty_embedding_model() {
    let _env_lock = env_lock();
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "   ");

    let error = load().expect_err("config load must fail for empty embedding model");

    assert!(error.to_string().contains(EMBEDDING_MODEL_ENV));
}

#[test]
fn load_rejects_zero_embedding_max_batch_size() {
    let _env_lock = env_lock();
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");
    let _batch_guard = EnvVarGuard::set(EMBEDDING_MAX_BATCH_SIZE_ENV, "0");

    let error = load().expect_err("config load must fail for non-positive max batch size");

    assert!(error.to_string().contains(EMBEDDING_MAX_BATCH_SIZE_ENV));
}

#[test]
fn load_allows_zero_embedding_worker_count() {
    let _env_lock = env_lock();
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");
    let _worker_guard = EnvVarGuard::set(EMBEDDING_WORKER_COUNT_ENV, "0");

    let config = load().expect("config load must allow disabling passive workers");

    assert_eq!(config.embedding_worker_count, 0);
}

#[test]
fn load_rejects_zero_database_pool_size() {
    let _env_lock = env_lock();
    let _model_guard = EnvVarGuard::set(EMBEDDING_MODEL_ENV, "custom-model");
    let _pool_guard = EnvVarGuard::set(DATABASE_POOL_SIZE_ENV, "0");

    let error = load().expect_err("config load must fail for non-positive database pool size");

    assert!(error.to_string().contains(DATABASE_POOL_SIZE_ENV));
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

#[test]
fn create_pool_with_max_size_uses_requested_pool_limit() {
    let temp_dir = unique_temp_dir();
    let database_path = temp_dir.join("ask.sqlite3");

    let pool = create_pool_with_max_size(&database_path.to_string_lossy(), 3)
        .expect("pool creation must succeed");

    assert_eq!(pool.max_size(), 3);

    drop(pool);
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

fn env_lock() -> MutexGuard<'static, ()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("env lock must not be poisoned")
}

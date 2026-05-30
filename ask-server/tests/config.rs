use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use ask_server::{
    config::{
        BIND_HOST_ENV, BIND_PORT_ENV, DEFAULT_BIND_HOST, DEFAULT_BIND_PORT, DEFAULT_SQLITE_PATH,
        SQLITE_PATH_ENV, load,
    },
    open_database,
};

#[test]
fn load_uses_defaults_when_env_is_missing() {
    let _sqlite_guard = EnvVarGuard::unset(SQLITE_PATH_ENV);
    let _host_guard = EnvVarGuard::unset(BIND_HOST_ENV);
    let _port_guard = EnvVarGuard::unset(BIND_PORT_ENV);

    let config = load().expect("config load must succeed");

    assert_eq!(config.sqlite_path, DEFAULT_SQLITE_PATH);
    assert_eq!(config.bind_host, DEFAULT_BIND_HOST);
    assert_eq!(config.bind_port, DEFAULT_BIND_PORT);
    assert_eq!(config.bind_address(), "0.0.0.0:3000");
}

#[test]
fn load_uses_env_overrides_when_present() {
    let _sqlite_guard = EnvVarGuard::set(SQLITE_PATH_ENV, "custom/ask.sqlite3");
    let _host_guard = EnvVarGuard::set(BIND_HOST_ENV, "127.0.0.1");
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "4123");

    let config = load().expect("config load must succeed");

    assert_eq!(config.sqlite_path, "custom/ask.sqlite3");
    assert_eq!(config.bind_host, "127.0.0.1");
    assert_eq!(config.bind_port, 4123);
    assert_eq!(config.bind_address(), "127.0.0.1:4123");
}

#[test]
fn load_rejects_invalid_bind_port() {
    let _port_guard = EnvVarGuard::set(BIND_PORT_ENV, "invalid-port");

    let error = load().expect_err("config load must fail for invalid port");

    assert!(error.to_string().contains(BIND_PORT_ENV));
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

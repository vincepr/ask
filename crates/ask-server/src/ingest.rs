use std::path::{Component, Path};

use anyhow::{Context, Result};
use ask_core::models::DEFAULT_FILE_PATTERN;
use regex::Regex;

/// Resolves the effective ingest include regex and validates it eagerly.
pub(crate) fn resolve_file_pattern(file_pattern: Option<&str>) -> Result<String, regex::Error> {
    let file_pattern = file_pattern.unwrap_or(DEFAULT_FILE_PATTERN);
    Regex::new(file_pattern)?;
    Ok(file_pattern.to_string())
}

/// Compiles a previously validated queue payload pattern.
pub(crate) fn compile_file_pattern(file_pattern: &str) -> Result<Regex> {
    Regex::new(file_pattern)
        .with_context(|| format!("invalid file pattern stored in queued job: {file_pattern}"))
}

/// Converts a candidate file path into a forward-slash relative path.
pub(crate) fn normalize_relative_path(root_path: &Path, candidate_path: &Path) -> Option<String> {
    let relative_path = candidate_path.strip_prefix(root_path).ok()?;
    let mut normalized = String::new();

    for component in relative_path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => {
                if !normalized.is_empty() {
                    normalized.push('/');
                }
                normalized.push_str(&part.to_string_lossy());
            }
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => return None,
        }
    }

    Some(normalized)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ask_core::models::DEFAULT_FILE_PATTERN;

    use super::{normalize_relative_path, resolve_file_pattern};

    #[test]
    fn resolve_file_pattern_uses_default_when_absent() {
        let resolved = resolve_file_pattern(None).expect("default regex must compile");

        assert_eq!(resolved, DEFAULT_FILE_PATTERN);
    }

    #[test]
    fn resolve_file_pattern_rejects_invalid_regex() {
        let err = resolve_file_pattern(Some("["))
            .expect_err("invalid regex must fail request validation");

        assert!(err.to_string().contains("regex parse error"));
    }

    #[test]
    fn normalize_relative_path_uses_forward_slashes() {
        let root = Path::new("/tmp/root");
        let candidate = Path::new("/tmp/root/src/nested/file.txt");

        let normalized = normalize_relative_path(root, candidate)
            .expect("candidate under root should normalize");

        assert_eq!(normalized, "src/nested/file.txt");
    }
}

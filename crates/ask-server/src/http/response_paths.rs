use std::path::{Path, PathBuf};

#[derive(Clone)]
pub(super) struct ResponsePathTranslator {
    roots: Vec<ResponsePathRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResponsePathRoot {
    pub(super) root: PathBuf,
    pub(super) display_root: String,
}

impl ResponsePathTranslator {
    pub(super) fn new<const N: usize>(roots: [ResponsePathRoot; N]) -> Self {
        let mut roots = roots.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            right
                .root
                .components()
                .count()
                .cmp(&left.root.components().count())
        });
        Self { roots }
    }

    pub(super) fn response_filepath(&self, stored_filepath: &str) -> String {
        let stored_path = Path::new(stored_filepath);
        for root in &self.roots {
            let Ok(relative) = stored_path.strip_prefix(&root.root) else {
                continue;
            };
            if relative.as_os_str().is_empty() {
                return root.display_root.clone();
            }
            return join_display_path(&root.display_root, relative);
        }

        stored_filepath.to_string()
    }
}

pub(super) fn display_path_root(path: &Path) -> String {
    let normalized = path.display().to_string().replace('\\', "/");
    if normalized.is_empty() {
        return ".".to_string();
    }

    trim_display_root(&normalized).to_string()
}

fn trim_display_root(path: &str) -> &str {
    let mut end = path.len();
    while end > 1 && path[..end].ends_with('/') && !is_windows_drive_root(&path[..end]) {
        end -= 1;
    }
    &path[..end]
}

fn is_windows_drive_root(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() == 3 && bytes[1] == b':' && bytes[2] == b'/'
}

fn join_display_path(display_root: &str, relative: &Path) -> String {
    let relative = normalize_relative_response_path(relative);
    if relative.is_empty() {
        return display_root.to_string();
    }

    match display_root {
        "/" => format!("/{relative}"),
        "." => format!("./{relative}"),
        _ if display_root.ends_with('/') => format!("{display_root}{relative}"),
        _ => format!("{display_root}/{relative}"),
    }
}

fn normalize_relative_response_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            std::path::Component::CurDir
            | std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempPathRoot {
        dir: PathBuf,
    }

    impl TempPathRoot {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "ask-response-path-test-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time must be after unix epoch")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).expect("temporary path root must be created");
            Self { dir }
        }
    }

    impl Drop for TempPathRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn translator(
        resource_root: &Path,
        data_root: &Path,
        resource_display_root: impl AsRef<Path>,
        data_display_root: impl AsRef<Path>,
    ) -> ResponsePathTranslator {
        ResponsePathTranslator::new([
            ResponsePathRoot {
                root: resource_root.canonicalize().expect("resource root exists"),
                display_root: display_path_root(resource_display_root.as_ref()),
            },
            ResponsePathRoot {
                root: data_root.canonicalize().expect("data root exists"),
                display_root: display_path_root(data_display_root.as_ref()),
            },
        ])
    }

    #[test]
    fn response_filepath_returns_resource_relative_path_for_matching_root() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        std::fs::create_dir_all(resource_root.join("nested")).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let translator = translator(&resource_root, &data_root, &resource_root, &data_root);
        let path = resource_root.join("nested").join("notes.md");
        std::fs::write(&path, "notes").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(
            translator.response_filepath(&stored),
            format!("{}/nested/notes.md", display_path_root(&resource_root))
        );
    }

    #[test]
    fn response_filepath_returns_data_relative_path_for_matching_root() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        std::fs::create_dir_all(&resource_root).unwrap();
        std::fs::create_dir_all(data_root.join("memory")).unwrap();
        let translator = translator(&resource_root, &data_root, &resource_root, &data_root);
        let path = data_root.join("memory").join("daily.md");
        std::fs::write(&path, "daily").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(
            translator.response_filepath(&stored),
            format!("{}/memory/daily.md", display_path_root(&data_root))
        );
    }

    #[test]
    fn response_filepath_uses_relative_resource_display_root() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        std::fs::create_dir_all(resource_root.join("nested")).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let translator = translator(&resource_root, &data_root, ".", ".data");
        let path = resource_root.join("nested").join("notes.md");
        std::fs::write(&path, "notes").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(translator.response_filepath(&stored), "./nested/notes.md");
    }

    #[test]
    fn response_filepath_uses_windows_display_root() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        std::fs::create_dir_all(resource_root.join("nested")).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let translator = translator(&resource_root, &data_root, r"C:\coding\ask\", ".data");
        let path = resource_root.join("nested").join("notes.md");
        std::fs::write(&path, "notes").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(
            translator.response_filepath(&stored),
            "C:/coding/ask/nested/notes.md"
        );
    }

    #[test]
    fn response_filepath_uses_relative_data_display_root() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        std::fs::create_dir_all(&resource_root).unwrap();
        std::fs::create_dir_all(data_root.join("memory")).unwrap();
        let translator = translator(&resource_root, &data_root, ".", ".data");
        let path = data_root.join("memory").join("daily.md");
        std::fs::write(&path, "daily").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(
            translator.response_filepath(&stored),
            ".data/memory/daily.md"
        );
    }

    #[test]
    fn response_filepath_prefers_longest_matching_root() {
        let temp = TempPathRoot::new();
        let data_root = temp.dir.join("data");
        let resource_root = data_root.join("resources");
        std::fs::create_dir_all(resource_root.join("nested")).unwrap();
        let translator = translator(&resource_root, &data_root, &resource_root, &data_root);
        let path = resource_root.join("nested").join("notes.md");
        std::fs::write(&path, "notes").unwrap();
        let stored = path.canonicalize().unwrap().to_string_lossy().into_owned();

        assert_eq!(
            translator.response_filepath(&stored),
            format!("{}/nested/notes.md", display_path_root(&resource_root))
        );
    }

    #[test]
    fn response_filepath_leaves_path_outside_configured_roots_unchanged() {
        let temp = TempPathRoot::new();
        let resource_root = temp.dir.join("resources");
        let data_root = temp.dir.join("data");
        let outside_root = temp.dir.join("outside");
        std::fs::create_dir_all(&resource_root).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        std::fs::create_dir_all(&outside_root).unwrap();
        let outside = outside_root.join("notes.md");
        std::fs::write(&outside, "outside").unwrap();
        let translator = translator(&resource_root, &data_root, &resource_root, &data_root);
        let stored = outside
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        assert_eq!(translator.response_filepath(&stored), stored);
    }
}

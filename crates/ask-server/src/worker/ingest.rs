use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow};
use ask_core::models::{Document, EmbeddingModel, IngestFolderPayload};
use ask_core::repository;
use ask_core::types::{ChunkType, DocCategory, JobType};
use ignore::WalkBuilder;
use regex::Regex;
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use super::chunking::{self, ChunkPlan};
use super::{JobContext, JobHandler, unix_now};
use crate::ingest;

#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

const GIT_IGNORED_FILE_EXTENSIONS: &[&str] = &[
    "7z", "bin", "bz2", "db", "dll", "dylib", "exe", "gif", "gz", "ico", "jpeg", "jpg", "pdf",
    "png", "rar", "so", "sqlite", "tar", "webp", "xz", "zip",
];
const CONTENT_PROBE_BYTES: usize = 8 * 1024;
const CONTENT_BYTE_BUDGET: usize = 1024 * 1024;
const UTF8_MAX_SCALAR_BYTES: usize = 4;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContentReadPlan {
    pub(super) content: Option<String>,
    pub(super) content_utf8: bool,
    pub(super) content_truncated: bool,
    pub(super) content_bytes_indexed: usize,
    pub(super) content_byte_budget: usize,
}

impl ContentReadPlan {
    fn filename_only() -> Self {
        Self {
            content: None,
            content_utf8: false,
            content_truncated: false,
            content_bytes_indexed: 0,
            content_byte_budget: CONTENT_BYTE_BUDGET,
        }
    }
}

pub(super) struct IngestFolderHandler;
pub(super) struct IngestFolderGitHandler;

impl JobHandler for IngestFolderHandler {
    fn job_type(&self) -> JobType {
        JobType::IngestFolder
    }

    fn process(&self, ctx: JobContext<'_>) -> Result<()> {
        let payload: IngestFolderPayload = serde_json::from_str(&ctx.entry.payload)
            .with_context(|| format!("failed to decode payload for job {}", ctx.entry.id))?;
        let root_path = Path::new(&payload.root_path);
        let file_pattern = ingest::compile_file_pattern(&payload.file_pattern)
            .with_context(|| format!("failed to compile file pattern for job {}", ctx.entry.id))?;

        if !root_path.is_dir() {
            warn!(
                job_id = ctx.entry.id,
                path = %payload.root_path,
                "ingest_folder path is missing or not a directory; completing job"
            );
            return Ok(());
        }

        info!(
            job_id = ctx.entry.id,
            path = %payload.root_path,
            file_pattern = %payload.file_pattern,
            "processing ingest_folder job"
        );

        let model = load_ingest_model(&ctx)?;
        ingest_walk_root(
            &ctx,
            root_path,
            root_path,
            &file_pattern,
            &model,
            &[],
            false,
        )
    }
}

impl JobHandler for IngestFolderGitHandler {
    fn job_type(&self) -> JobType {
        JobType::IngestFolderGit
    }

    fn process(&self, ctx: JobContext<'_>) -> Result<()> {
        let payload: IngestFolderPayload = serde_json::from_str(&ctx.entry.payload)
            .with_context(|| format!("failed to decode payload for job {}", ctx.entry.id))?;
        let root_path = Path::new(&payload.root_path);
        let file_pattern = ingest::compile_file_pattern(&payload.file_pattern)
            .with_context(|| format!("failed to compile file pattern for job {}", ctx.entry.id))?;

        if !root_path.is_dir() {
            warn!(
                job_id = ctx.entry.id,
                path = %payload.root_path,
                "ingest_folder_git path is missing or not a directory; completing job"
            );
            return Ok(());
        }

        info!(
            job_id = ctx.entry.id,
            path = %payload.root_path,
            file_pattern = %payload.file_pattern,
            "processing ingest_folder_git job"
        );

        let model = load_ingest_model(&ctx)?;
        let plan = build_git_ingest_plan(root_path)
            .with_context(|| format!("failed to discover git roots under {}", payload.root_path))?;

        for repo_root in &plan.repo_roots {
            ingest_git_repo(&ctx, root_path, repo_root, &file_pattern, &model)?;
        }

        if let Some(fallback_root) = plan.fallback_root.as_deref() {
            ingest_walk_root(
                &ctx,
                root_path,
                fallback_root,
                &file_pattern,
                &model,
                &plan.skipped_walk_roots,
                false,
            )?;
        }

        Ok(())
    }
}

fn load_ingest_model(ctx: &JobContext<'_>) -> Result<EmbeddingModel> {
    let conn = ctx.pool.get().with_context(|| {
        format!(
            "failed to acquire connection to load model {} for job {}",
            ctx.ingest_model_id, ctx.entry.id
        )
    })?;

    repository::find_model_by_id(&conn, ctx.ingest_model_id)?.with_context(|| {
        format!(
            "embedding model {} not found for job {}",
            ctx.ingest_model_id, ctx.entry.id
        )
    })
}

fn ingest_git_repo(
    ctx: &JobContext<'_>,
    request_root: &Path,
    repo_root: &Path,
    file_pattern: &Regex,
    model: &EmbeddingModel,
) -> Result<()> {
    let tracked_paths = git_list_tracked_files(repo_root)
        .with_context(|| format!("failed to list tracked files for {}", repo_root.display()))?;

    for tracked_path in tracked_paths {
        let candidate_path = repo_root.join(tracked_path);
        ingest_candidate_file(
            ctx,
            request_root,
            &candidate_path,
            file_pattern,
            model,
            true,
        )?;
    }

    Ok(())
}

fn ingest_walk_root(
    ctx: &JobContext<'_>,
    request_root: &Path,
    walk_root: &Path,
    file_pattern: &Regex,
    model: &EmbeddingModel,
    skipped_roots: &[PathBuf],
    apply_git_filters: bool,
) -> Result<()> {
    let walk_root = walk_root.to_path_buf();
    let skipped_roots = skipped_roots.to_vec();
    let walker = WalkBuilder::new(&walk_root)
        .follow_links(false)
        .filter_entry(move |entry| {
            should_visit_walk_entry(entry.path(), &walk_root, &skipped_roots)
        })
        .build();

    for entry_result in walker {
        let dir_entry = match entry_result {
            Ok(dir_entry) => dir_entry,
            Err(err) => {
                warn!(
                    job_id = ctx.entry.id,
                    error = %err,
                    "failed to walk directory entry; continuing"
                );
                continue;
            }
        };

        let file_type = match dir_entry.file_type() {
            Some(file_type) => file_type,
            None => {
                warn!(
                    job_id = ctx.entry.id,
                    path = ?dir_entry.path(),
                    "failed to read directory entry type; continuing"
                );
                continue;
            }
        };

        if !file_type.is_file() {
            continue;
        }

        ingest_candidate_file(
            ctx,
            request_root,
            &dir_entry.into_path(),
            file_pattern,
            model,
            apply_git_filters,
        )?;
    }

    Ok(())
}

fn ingest_candidate_file(
    ctx: &JobContext<'_>,
    request_root: &Path,
    candidate_path: &Path,
    file_pattern: &Regex,
    model: &EmbeddingModel,
    apply_git_filters: bool,
) -> Result<()> {
    let relative_path = match ingest::normalize_relative_path(request_root, candidate_path) {
        Some(relative_path) => relative_path,
        None => return Ok(()),
    };

    if !file_pattern.is_match(&relative_path) {
        return Ok(());
    }

    if apply_git_filters && should_skip_git_candidate(candidate_path) {
        return Ok(());
    }

    let canonical_path = match std::fs::canonicalize(candidate_path) {
        Ok(path) => path,
        Err(err) => {
            warn!(
                job_id = ctx.entry.id,
                path = ?candidate_path,
                error = %err,
                "failed to canonicalize file path; continuing"
            );
            return Ok(());
        }
    };

    let metadata = match std::fs::metadata(&canonical_path) {
        Ok(metadata) => metadata,
        Err(err) => {
            warn!(
                job_id = ctx.entry.id,
                path = ?canonical_path,
                error = %err,
                "failed to read file metadata; continuing"
            );
            return Ok(());
        }
    };

    if !metadata.is_file() {
        return Ok(());
    }

    let now = unix_now();
    let filepath = canonical_path.to_string_lossy().into_owned();
    let file_type = canonical_path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("")
        .to_string();
    let file_modified_at = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(now);
    let file_size = metadata.len() as i64;
    let file_hash = match hash_file(&canonical_path) {
        Ok(file_hash) => file_hash,
        Err(err) => {
            warn!(
                job_id = ctx.entry.id,
                path = ?canonical_path,
                error = %err,
                "failed to hash file bytes; continuing"
            );
            return Ok(());
        }
    };
    let content_plan = match read_content_prefix(&canonical_path) {
        Ok(content_plan) => content_plan,
        Err(err) => {
            warn!(
                job_id = ctx.entry.id,
                path = ?canonical_path,
                error = %err,
                "failed to read file content prefix; queueing filename-only embedding"
            );
            ContentReadPlan::filename_only()
        }
    };
    let planned = plan_pending_embeddings_for_read_plan(&canonical_path, &content_plan, model);

    let mut conn = ctx
        .pool
        .get()
        .with_context(|| format!("failed to acquire connection while ingesting {filepath}"))?;

    let doc = Document {
        id: 0,
        filepath: filepath.clone(),
        file_type,
        doc_category: DocCategory::Resource,
        file_modified_at,
        file_size,
        file_hash,
        metadata_json: planned.metadata_json,
        updated_at: now,
    };

    repository::upsert_document_and_replace_pending_embeddings(
        &mut conn,
        &doc,
        model.id,
        &planned.chunks,
        now,
    )
    .with_context(|| format!("failed to ingest document for {filepath}"))?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitIngestPlan {
    repo_roots: Vec<PathBuf>,
    fallback_root: Option<PathBuf>,
    skipped_walk_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoMarker {
    Repo,
    Submodule,
}

fn build_git_ingest_plan(root_path: &Path) -> Result<GitIngestPlan> {
    if let Some(repo_root) = discover_enclosing_repo_root(root_path)? {
        return Ok(GitIngestPlan {
            repo_roots: vec![repo_root],
            fallback_root: None,
            skipped_walk_roots: Vec::new(),
        });
    }

    let mut repo_roots = Vec::new();
    let mut skipped_walk_roots = Vec::new();
    let mut stack = vec![root_path.to_path_buf()];

    while let Some(dir) = stack.pop() {
        match detect_repo_marker(&dir)? {
            Some(RepoMarker::Repo) => {
                repo_roots.push(dir);
                continue;
            }
            Some(RepoMarker::Submodule) => {
                skipped_walk_roots.push(dir);
                continue;
            }
            None => {}
        }

        for entry in std::fs::read_dir(&dir)
            .with_context(|| format!("failed to read directory {}", dir.display()))?
        {
            let entry = entry.with_context(|| {
                format!("failed to inspect directory entry under {}", dir.display())
            })?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("failed to read entry type under {}", dir.display()))?;
            if file_type.is_symlink() || !file_type.is_dir() {
                continue;
            }
            stack.push(entry.path());
        }
    }

    skipped_walk_roots.extend(repo_roots.iter().cloned());

    Ok(GitIngestPlan {
        repo_roots,
        fallback_root: Some(root_path.to_path_buf()),
        skipped_walk_roots,
    })
}

fn discover_enclosing_repo_root(root_path: &Path) -> Result<Option<PathBuf>> {
    let output = match Command::new("git")
        .arg("-C")
        .arg(root_path)
        .args(["rev-parse", "--show-toplevel"])
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to invoke git for {}", root_path.display()));
        }
    };

    if !output.status.success() {
        return Ok(None);
    }

    let repo_root = String::from_utf8(output.stdout)
        .context("git rev-parse returned non-utf8 path")?
        .trim()
        .to_string();
    if repo_root.is_empty() {
        return Ok(None);
    }

    Ok(Some(PathBuf::from(repo_root)))
}

fn detect_repo_marker(dir: &Path) -> Result<Option<RepoMarker>> {
    let git_path = dir.join(".git");
    let metadata = match std::fs::symlink_metadata(&git_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to inspect git metadata at {}", git_path.display())
            });
        }
    };

    if metadata.is_dir() {
        if !git_path.join("HEAD").is_file() {
            return Ok(None);
        }
        return Ok(Some(RepoMarker::Repo));
    }

    if !metadata.is_file() {
        return Ok(None);
    }

    let gitdir = parse_gitdir_pointer(dir, &git_path)?;
    if gitdir_points_to_submodule(&gitdir) {
        return Ok(Some(RepoMarker::Submodule));
    }

    Ok(Some(RepoMarker::Repo))
}

fn parse_gitdir_pointer(dir: &Path, git_file: &Path) -> Result<PathBuf> {
    let raw = std::fs::read_to_string(git_file)
        .with_context(|| format!("failed to read {}", git_file.display()))?;
    let gitdir = raw
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("invalid gitdir pointer in {}", git_file.display()))?;

    let gitdir = Path::new(gitdir);
    if gitdir.is_absolute() {
        return Ok(gitdir.to_path_buf());
    }

    Ok(dir.join(gitdir))
}

fn gitdir_points_to_submodule(gitdir: &Path) -> bool {
    let mut saw_dot_git = false;

    for component in gitdir.components() {
        let std::path::Component::Normal(part) = component else {
            continue;
        };

        if saw_dot_git && part == "modules" {
            return true;
        }

        saw_dot_git = part == ".git";
    }

    false
}

fn git_list_tracked_files(repo_root: &Path) -> Result<Vec<PathBuf>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["ls-files", "--cached", "-z"])
        .output()
        .with_context(|| format!("failed to invoke git for {}", repo_root.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "git ls-files failed for {}: {}",
            repo_root.display(),
            stderr.trim()
        ));
    }

    Ok(parse_git_path_list(&output.stdout))
}

fn parse_git_path_list(stdout: &[u8]) -> Vec<PathBuf> {
    stdout
        .split(|byte| *byte == b'\0')
        .filter(|path| !path.is_empty())
        .map(path_from_git_output)
        .collect()
}

fn path_from_git_output(raw: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        PathBuf::from(std::ffi::OsString::from_vec(raw.to_vec()))
    }

    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(raw).into_owned())
    }
}

fn should_visit_walk_entry(path: &Path, walk_root: &Path, skipped_roots: &[PathBuf]) -> bool {
    if path == walk_root {
        return true;
    }

    !skipped_roots.iter().any(|root| path.starts_with(root))
}

fn should_skip_git_candidate(path: &Path) -> bool {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);

    match extension.as_deref() {
        Some(extension) => GIT_IGNORED_FILE_EXTENSIONS.contains(&extension),
        None => false,
    }
}

pub(super) fn queue_pending_embeddings_for_document(
    conn: &rusqlite::Connection,
    path: &Path,
    doc_id: i64,
    model: &EmbeddingModel,
    now: i64,
) -> Result<()> {
    let filepath = path.to_string_lossy();

    let content_plan = match read_content_prefix(path) {
        Ok(content_plan) => content_plan,
        Err(err) => {
            warn!(
                path = %filepath,
                error = %err,
                "queueing filename-only embedding for unreadable file"
            );
            ContentReadPlan::filename_only()
        }
    };
    let chunk_refs = plan_pending_embeddings_for_read_plan(path, &content_plan, model).chunks;

    repository::insert_pending_embeddings(conn, doc_id, model.id, &chunk_refs, now)
        .with_context(|| format!("failed to queue embeddings for {filepath}"))?;

    Ok(())
}

pub(super) fn replan_document_from_path(
    conn: &mut rusqlite::Connection,
    doc: &Document,
    model: &EmbeddingModel,
    now: i64,
) -> Result<()> {
    let path = Path::new(&doc.filepath);
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read file metadata for {}", doc.filepath))?;
    let file_modified_at = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(now);
    let file_hash = hash_file(path)
        .with_context(|| format!("failed to hash document bytes for {}", doc.filepath))?;
    let content_plan = read_content_prefix(path)
        .with_context(|| format!("failed to read content prefix for {}", doc.filepath))?;
    let planned = plan_pending_embeddings_for_read_plan(path, &content_plan, model);
    let updated_doc = Document {
        id: doc.id,
        filepath: doc.filepath.clone(),
        file_type: doc.file_type.clone(),
        doc_category: doc.doc_category,
        file_modified_at,
        file_size: metadata.len() as i64,
        file_hash,
        metadata_json: planned.metadata_json,
        updated_at: now,
    };

    repository::upsert_document_and_replace_pending_embeddings(
        conn,
        &updated_doc,
        model.id,
        &planned.chunks,
        now,
    )?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedEmbeddings {
    pub(super) chunks: Vec<(ChunkType, i64, i64)>,
    pub(super) metadata_json: String,
}

fn plan_pending_embeddings_for_read_plan(
    path: &Path,
    content_plan: &ContentReadPlan,
    model: &EmbeddingModel,
) -> PlannedEmbeddings {
    plan_pending_embeddings_for_content(
        path,
        content_plan.content.as_deref(),
        content_plan.content_truncated,
        content_plan.content_bytes_indexed,
        content_plan.content_byte_budget,
        model,
    )
}

pub(super) fn plan_pending_embeddings_for_content(
    path: &Path,
    content: Option<&str>,
    content_truncated: bool,
    content_bytes_indexed: usize,
    content_byte_budget: usize,
    model: &EmbeddingModel,
) -> PlannedEmbeddings {
    let mut chunk_refs = vec![(ChunkType::Filename, 0, 0)];

    let Some(content) = content.filter(|content| !content.is_empty()) else {
        return PlannedEmbeddings {
            chunks: chunk_refs,
            metadata_json: metadata_json(
                "structure",
                0,
                false,
                content_truncated,
                content_bytes_indexed,
                content_byte_budget,
            ),
        };
    };

    let plan = chunking::plan_chunks(
        path,
        content,
        model.chunk_size as usize,
        model.chunk_overlap as usize,
    );
    chunk_refs.extend(
        plan.spans
            .iter()
            .map(|span| (ChunkType::Content, span.start as i64, span.end as i64)),
    );

    PlannedEmbeddings {
        chunks: chunk_refs,
        metadata_json: plan_metadata_json(
            &plan,
            content_truncated,
            content_bytes_indexed,
            content_byte_budget,
        ),
    }
}

fn plan_metadata_json(
    plan: &ChunkPlan,
    content_truncated: bool,
    content_bytes_indexed: usize,
    content_byte_budget: usize,
) -> String {
    metadata_json(
        plan.strategy,
        plan.spans.len(),
        true,
        content_truncated,
        content_bytes_indexed,
        content_byte_budget,
    )
}

fn metadata_json(
    strategy: &str,
    chunk_count: usize,
    content_utf8: bool,
    content_truncated: bool,
    content_bytes_indexed: usize,
    content_byte_budget: usize,
) -> String {
    serde_json::json!({
        "strategy": strategy,
        "planned_chunk_count": chunk_count,
        "content_utf8": content_utf8,
        "content_truncated": content_truncated,
        "content_bytes_indexed": content_bytes_indexed,
        "content_byte_budget": content_byte_budget,
    })
    .to_string()
}

#[cfg(test)]
pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

pub(super) fn hash_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hex_digest(hasher.finalize()))
}

pub(super) fn read_content_prefix(path: &Path) -> Result<ContentReadPlan> {
    read_content_prefix_with_budget(path, CONTENT_BYTE_BUDGET)
}

pub(super) fn read_content_prefix_with_budget(
    path: &Path,
    content_byte_budget: usize,
) -> Result<ContentReadPlan> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;
    let file_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    let read_limit = content_byte_budget.saturating_add(UTF8_MAX_SCALAR_BYTES - 1);
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut bytes = Vec::with_capacity(read_limit.min(file_len));
    let mut handle = (&mut file).take(read_limit as u64);
    handle
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read content prefix from {}", path.display()))?;

    if bytes
        .iter()
        .take(CONTENT_PROBE_BYTES)
        .any(|byte| *byte == 0)
    {
        return Ok(ContentReadPlan {
            content: None,
            content_utf8: false,
            content_truncated: file_len > bytes.len(),
            content_bytes_indexed: 0,
            content_byte_budget,
        });
    }

    let read_truncated = file_len > bytes.len();
    let Some(content) = decode_bounded_utf8_prefix(&bytes, content_byte_budget, read_truncated)
    else {
        return Ok(ContentReadPlan {
            content: None,
            content_utf8: false,
            content_truncated: read_truncated,
            content_bytes_indexed: 0,
            content_byte_budget,
        });
    };
    let content_bytes_indexed = content.len();
    let content = (!content.is_empty()).then_some(content);

    Ok(ContentReadPlan {
        content,
        content_utf8: content_bytes_indexed > 0,
        content_truncated: content_bytes_indexed < file_len,
        content_bytes_indexed,
        content_byte_budget,
    })
}

fn decode_bounded_utf8_prefix(
    bytes: &[u8],
    content_byte_budget: usize,
    read_truncated: bool,
) -> Option<String> {
    if bytes.is_empty() || content_byte_budget == 0 {
        return Some(String::new());
    }

    match std::str::from_utf8(bytes) {
        Ok(content) => {
            let end = floor_char_boundary(content, content.len().min(content_byte_budget));
            Some(content[..end].to_string())
        }
        Err(err) if err.error_len().is_none() && read_truncated => {
            let valid = std::str::from_utf8(&bytes[..err.valid_up_to()])
                .expect("valid_up_to must identify valid UTF-8");
            let end = floor_char_boundary(valid, valid.len().min(content_byte_budget));
            Some(valid[..end].to_string())
        }
        Err(_) => None,
    }
}

fn floor_char_boundary(content: &str, mut index: usize) -> usize {
    index = index.min(content.len());
    while index > 0 && !content.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    let bytes = digest.as_ref();
    let mut hash = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut hash, "{byte:02x}").expect("writing to String cannot fail");
    }
    hash
}

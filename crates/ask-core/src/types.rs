use std::fmt;

/// Whether a document lives in the knowledge directory or an arbitrary resource path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocCategory {
    KnowledgeFile,
    Resource,
}

impl DocCategory {
    pub const KNOWLEDGE: &'static str = "knowledge_file";
    pub const RESOURCE: &'static str = "resource";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::KnowledgeFile => Self::KNOWLEDGE,
            Self::Resource => Self::RESOURCE,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            Self::KNOWLEDGE => Some(Self::KnowledgeFile),
            Self::RESOURCE => Some(Self::Resource),
            _ => None,
        }
    }
}

impl fmt::Display for DocCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Embedding state for a (document, model, chunk) triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedState {
    Pending,
    Embedded,
    Stale,
}

impl EmbedState {
    pub const PENDING: &'static str = "pending";
    pub const EMBEDDED: &'static str = "embedded";
    pub const STALE: &'static str = "stale";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => Self::PENDING,
            Self::Embedded => Self::EMBEDDED,
            Self::Stale => Self::STALE,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            Self::PENDING => Some(Self::Pending),
            Self::EMBEDDED => Some(Self::Embedded),
            Self::STALE => Some(Self::Stale),
            _ => None,
        }
    }
}

impl fmt::Display for EmbedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an embedding row represents a content chunk or a filename.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkType {
    Content,
    Filename,
}

impl ChunkType {
    pub const CONTENT: &'static str = "content";
    pub const FILENAME: &'static str = "filename";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Content => Self::CONTENT,
            Self::Filename => Self::FILENAME,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            Self::CONTENT => Some(Self::Content),
            Self::FILENAME => Some(Self::Filename),
            _ => None,
        }
    }
}

impl fmt::Display for ChunkType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Discriminant for every async job variant stored in the job_queue table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    IngestFolder,
}

impl JobType {
    pub const INGEST_FOLDER: &'static str = "ingest_folder";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IngestFolder => Self::INGEST_FOLDER,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            Self::INGEST_FOLDER => Some(Self::IngestFolder),
            _ => None,
        }
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

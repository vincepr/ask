use serde::{Deserialize, Serialize};

use crate::types::JobType;

/// A single row in the job_queue table.
///
/// A unique index on `(job_type, payload)` ensures that only one
/// pending/active job exists for a given type+payload pair, preventing
/// parallel workers from duplicating the same work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobQueueEntry {
    pub id: i64,
    pub job_type: JobType,
    pub payload: String,
    pub claimed_at: Option<i64>,
    pub created_at: i64,
}

/// Payload for an IngestFolder job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestFolderPayload {
    pub root_path: String,
}

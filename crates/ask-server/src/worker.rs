use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use ask_core::models::IngestFolderPayload;
use ask_core::repository;
use ask_core::types::JobType;

use crate::DbPool;

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background task that polls for unclaimed jobs, processes them, and
/// keeps their heartbeats fresh until completion.
pub fn spawn(pool: DbPool) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            if let Err(e) = tick(&pool) {
                eprintln!("worker tick failed: {e:#}");
            }
        }
    });
}

fn tick(pool: &DbPool) -> Result<()> {
    let now = unix_now();

    let mut conn = pool.get()?;
    let entry = repository::claim_job(&mut conn, now)?;

    let entry = match entry {
        Some(e) => e,
        None => return Ok(()),
    };

    println!("claimed job {} ({})", entry.id, entry.job_type.as_str());

    match entry.job_type {
        JobType::IngestFolder => process_ingest_folder(pool, entry.id, &entry.payload),
    }
}

fn process_ingest_folder(pool: &DbPool, job_id: i64, payload_json: &str) -> Result<()> {
    let payload: IngestFolderPayload = serde_json::from_str(payload_json)?;
    let root_path = Path::new(&payload.root_path);

    if !root_path.is_dir() {
        eprintln!(
            "ingest_folder path does not exist (skipped): {}",
            payload.root_path
        );
        let conn = pool.get()?;
        repository::complete_job(&conn, job_id)?;
        return Ok(());
    }

    println!("processing ingest_folder: {}", payload.root_path);

    if let Ok(mut entries) = std::fs::read_dir(root_path) {
        while let Some(Ok(_entry)) = entries.next() {
            let now = unix_now();
            let conn = pool.get()?;
            repository::update_heartbeat(&conn, job_id, now)?;
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    let conn = pool.get()?;
    repository::complete_job(&conn, job_id)?;
    println!("completed job {job_id}");

    Ok(())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64
}

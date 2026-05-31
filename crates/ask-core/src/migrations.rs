use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Migration {
    version: i64,
    required_actions: Option<&'static str>,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        required_actions: None,
        sql: include_str!("../migrations/0001_bootstrap_migration_system.sql"),
    },
    Migration {
        version: 2,
        required_actions: None,
        sql: include_str!("../migrations/0002_create_domain_tables.sql"),
    },
    Migration {
        version: 3,
        required_actions: None,
        sql: include_str!("../migrations/0003_create_job_queue.sql"),
    },
    Migration {
        version: 4,
        required_actions: None,
        sql: include_str!("../migrations/0004_simplify_job_queue_claim.sql"),
    },
    Migration {
        version: 5,
        required_actions: None,
        sql: include_str!("../migrations/0005_create_embedding_search_state.sql"),
    },
    Migration {
        version: 6,
        required_actions: None,
        sql: include_str!("../migrations/0006_rebuild_embedding_model_identity.sql"),
    },
    Migration {
        version: 7,
        required_actions: None,
        sql: include_str!("../migrations/0007_create_document_filepath_search.sql"),
    },
];

/// Applies every embedded migration that has not yet been recorded.
///
/// Migrations are validated and then executed in ascending version order inside
/// individual transactions.
///
/// # Errors
///
/// Returns an error if the tracking table cannot be created, migrations are not
/// strictly ordered, a migration fails to execute, or a migration record cannot
/// be committed.
pub fn apply_pending_migrations(connection: &mut Connection) -> Result<usize> {
    create_migrations_table(connection)?;
    validate_migrations(MIGRATIONS)?;

    let applied_versions = load_applied_versions(connection)?;
    let mut applied_count = 0;

    for migration in MIGRATIONS {
        if applied_versions.contains(&migration.version) {
            continue;
        }

        let transaction = connection
            .transaction()
            .context("failed to start migration transaction")?;

        transaction
            .execute_batch(migration.sql)
            .with_context(|| format!("failed to apply migration {}", migration.version))?;

        transaction
            .execute(
                "INSERT INTO migrations (version, required_actions) VALUES (?1, ?2)",
                params![migration.version, migration.required_actions],
            )
            .with_context(|| format!("failed to record migration {}", migration.version))?;

        transaction
            .commit()
            .context("failed to commit migration transaction")?;
        applied_count += 1;
    }

    Ok(applied_count)
}

fn create_migrations_table(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS migrations (
                version INTEGER PRIMARY KEY NOT NULL,
                required_actions TEXT NULL
            );
            ",
        )
        .context("failed to create migrations tracking table")
}

fn load_applied_versions(connection: &Connection) -> Result<BTreeSet<i64>> {
    let mut statement = connection
        .prepare("SELECT version FROM migrations ORDER BY version ASC")
        .context("failed to prepare applied migrations query")?;

    let versions = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .context("failed to query applied migrations")?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .context("failed to read applied migration versions")?;

    Ok(versions)
}

fn validate_migrations(migrations: &[Migration]) -> Result<()> {
    let mut previous_version = None;

    for migration in migrations {
        if let Some(last_version) = previous_version {
            ensure!(
                migration.version > last_version,
                "migration versions must be strictly increasing: {} came after {}",
                migration.version,
                last_version
            );
        }

        ensure!(
            migration.version > 0,
            "migration versions must be positive: {}",
            migration.version
        );

        previous_version = Some(migration.version);
    }

    Ok(())
}

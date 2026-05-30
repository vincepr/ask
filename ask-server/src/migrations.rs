use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Migration {
    version: i64,
    required_actions: Option<&'static str>,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    required_actions: None,
    sql: include_str!("../migrations/0001_bootstrap_migration_system.sql"),
}];

pub(crate) fn apply_pending_migrations(connection: &mut Connection) -> Result<usize> {
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

#[cfg(test)]
mod tests {
    use super::{Migration, apply_pending_migrations, validate_migrations};
    use rusqlite::Connection;

    #[test]
    fn applies_migrations_only_once() {
        let mut connection = Connection::open_in_memory().expect("in-memory database must open");

        let first_run =
            apply_pending_migrations(&mut connection).expect("first migration run must succeed");
        let second_run =
            apply_pending_migrations(&mut connection).expect("second migration run must succeed");

        let applied_total: i64 = connection
            .query_row("SELECT COUNT(*) FROM migrations", [], |row| row.get(0))
            .expect("migration count query must succeed");

        assert_eq!(first_run, 1);
        assert_eq!(second_run, 0);
        assert_eq!(applied_total, 1);
    }

    #[test]
    fn stores_required_actions_as_null_when_not_needed() {
        let mut connection = Connection::open_in_memory().expect("in-memory database must open");

        apply_pending_migrations(&mut connection).expect("migration run must succeed");

        let required_actions: Option<String> = connection
            .query_row(
                "SELECT required_actions FROM migrations WHERE version = 1",
                [],
                |row| row.get(0),
            )
            .expect("required_actions query must succeed");

        assert_eq!(required_actions, None);
    }

    #[test]
    fn rejects_non_increasing_versions() {
        let migrations = [
            Migration {
                version: 2,
                required_actions: None,
                sql: "SELECT 1;",
            },
            Migration {
                version: 2,
                required_actions: None,
                sql: "SELECT 1;",
            },
        ];

        let error = validate_migrations(&migrations).expect_err("validation must fail");

        assert!(error.to_string().contains("strictly increasing"));
    }
}

use ask_server::migrations::apply_pending_migrations;
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

    assert_eq!(first_run, 2);
    assert_eq!(second_run, 0);
    assert_eq!(applied_total, 2);
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

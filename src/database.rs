//! Common durability contract for every SQLite writer.
use rusqlite::{Connection, Error};
use std::time::Duration;

/// Set and verify WAL plus per-connection FULL synchronization before writing.
/// SQLite and the filesystem still have to honor their documented sync contract.
pub(crate) fn configure_writer(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    let mode: String = conn.query_row("PRAGMA main.journal_mode=WAL", [], |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(unavailable("SQLite could not enable WAL journal mode"));
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = conn.query_row("PRAGMA main.synchronous", [], |r| r.get(0))?;
    if synchronous != 2 {
        return Err(unavailable("SQLite could not enable FULL synchronization"));
    }
    Ok(())
}

fn unavailable(message: &str) -> Error {
    Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
        Some(message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_enables_wal_and_restores_full_synchronization() {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("durable.sqlite")).unwrap();
        conn.pragma_update(None, "synchronous", "OFF").unwrap();
        configure_writer(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        let sync: i64 = conn
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        assert_eq!(sync, 2);
    }

    #[test]
    fn writer_refuses_a_database_that_cannot_use_wal() {
        let conn = Connection::open_in_memory().unwrap();
        let error = configure_writer(&conn).unwrap_err();
        assert!(error.to_string().contains("could not enable WAL"));
        assert!(
            !conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table')",
                    [],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap()
        );
    }

    #[test]
    fn all_writer_entrypoints_refuse_non_wal_databases() {
        let pools = [("rust".into(), 1)].into();
        let capacity = crate::scheduler::Capacity {
            pools,
            hosts: [(
                "host".into(),
                crate::scheduler::Host {
                    slots: 1,
                    runner_ids: Default::default(),
                    pools: [("rust".into(), 1)].into(),
                },
            )]
            .into(),
        };
        let errors = [
            crate::store::Store::open(":memory:")
                .err()
                .unwrap()
                .to_string(),
            crate::webhook::Inbox::open(":memory:")
                .err()
                .unwrap()
                .to_string(),
            crate::landing::LandingJournal::open(":memory:")
                .err()
                .unwrap()
                .to_string(),
            crate::scheduler::Scheduler::open(":memory:", capacity)
                .err()
                .unwrap()
                .to_string(),
        ];
        assert!(
            errors
                .iter()
                .all(|error| error.contains("could not enable WAL"))
        );
    }
}

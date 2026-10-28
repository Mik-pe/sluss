//! A process-shared, durable gate for provider rate limits. No credentials are stored.
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now_ms() -> Result<i64, ()> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?
        .as_millis();
    i64::try_from(value).map_err(|_| ())
}

pub(crate) fn initialize(path: &Path) -> Result<(), ()> {
    let conn = Connection::open(path).map_err(|_| ())?;
    crate::database::configure_writer(&conn).map_err(|_| ())?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS github_rate_limit_gate (id INTEGER PRIMARY KEY CHECK(id = 1), observed_ms INTEGER NOT NULL CHECK(observed_ms >= 0), until_ms INTEGER NOT NULL CHECK(until_ms >= observed_ms));").map_err(|_| ())?;
    Ok(())
}

pub(crate) fn blocked(path: &Path) -> Result<bool, ()> {
    let conn =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|_| ())?;
    conn.busy_timeout(Duration::from_secs(5)).map_err(|_| ())?;
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT observed_ms,until_ms FROM github_rate_limit_gate WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(|_| ())?;
    let now = now_ms()?;
    match row {
        None => Ok(false),
        Some((observed, until)) if observed >= 0 && until >= observed => {
            Ok(now < observed || now < until)
        }
        _ => Err(()),
    }
}

pub(crate) fn defer(path: &Path, delay: Duration) -> Result<(), ()> {
    let mut conn =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE).map_err(|_| ())?;
    crate::database::configure_writer(&conn).map_err(|_| ())?;
    let now = now_ms()?;
    let delta = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    let until = now.saturating_add(delta);
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| ())?;
    tx.execute("INSERT INTO github_rate_limit_gate(id,observed_ms,until_ms) VALUES(1,?1,?2) ON CONFLICT(id) DO UPDATE SET observed_ms=MAX(observed_ms,excluded.observed_ms), until_ms=MAX(until_ms,excluded.until_ms)", params![now,until]).map_err(|_| ())?;
    tx.commit().map_err(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_gate_never_shortens_and_fails_closed_on_clock_regression() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        initialize(&path).unwrap();
        assert!(!blocked(&path).unwrap());
        defer(&path, Duration::from_secs(120)).unwrap();
        let conn = Connection::open(&path).unwrap();
        let first: i64 = conn
            .query_row("SELECT until_ms FROM github_rate_limit_gate", [], |r| {
                r.get(0)
            })
            .unwrap();
        defer(&path, Duration::from_secs(1)).unwrap();
        let next: i64 = conn
            .query_row("SELECT until_ms FROM github_rate_limit_gate", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(first, next);
        assert!(blocked(&path).unwrap());
        let future = now_ms().unwrap() + 10_000;
        conn.execute(
            "UPDATE github_rate_limit_gate SET observed_ms=?1,until_ms=?1",
            [future],
        )
        .unwrap();
        assert!(blocked(&path).unwrap());
        drop(conn);
        std::fs::remove_file(&path).unwrap();
        assert!(blocked(&path).is_err());
    }
}

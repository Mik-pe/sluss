//! Authenticated webhook intake. Events schedule reconciliation; they do not
//! authorize changes. A configured worker reconciles the current provider state.
use std::{path::PathBuf, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use hmac::{Hmac, Mac};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct AppState {
    secret: Arc<[u8]>,
    db: PathBuf,
    scope: Option<(i64, i64)>,
}

/// Construct a webhook-only router. TLS and public ingress belong at a trusted proxy.
pub fn router(secret: Vec<u8>, db: PathBuf) -> Result<Router, &'static str> {
    router_with_scope(secret, db, None)
}
pub fn router_with_scope(
    secret: Vec<u8>,
    db: PathBuf,
    scope: Option<(i64, i64)>,
) -> Result<Router, &'static str> {
    if secret.is_empty() {
        return Err("webhook secret must not be empty");
    }
    Ok(Router::new()
        .route("/webhook", post(receive))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(AppState {
            secret: secret.into(),
            db,
            scope,
        }))
}

/// GitHub signs the raw payload, not the delivery or event headers. Delivery
/// IDs deduplicate retries, but do not provide cryptographic replay protection.
pub fn verify_signature(secret: &[u8], signature: &str, payload: &[u8]) -> bool {
    let Some(hex_signature) = signature.strip_prefix("sha256=") else {
        return false;
    };
    if hex_signature.len() != 64 {
        return false;
    }
    let Ok(bytes) = hex::decode(hex_signature) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(payload);
    mac.verify_slice(&bytes).is_ok()
}

#[derive(Deserialize)]
struct Entity {
    id: i64,
}
#[derive(Deserialize)]
struct Envelope {
    installation: Entity,
    repository: Option<Entity>,
}

async fn receive(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let Some(signature) = get("x-hub-signature-256") else {
        return StatusCode::UNAUTHORIZED;
    };
    if !verify_signature(&state.secret, signature, &body) {
        return StatusCode::UNAUTHORIZED;
    }
    let (Some(delivery), Some(event)) = (get("x-github-delivery"), get("x-github-event")) else {
        return StatusCode::BAD_REQUEST;
    };
    if delivery.is_empty()
        || delivery.len() > 128
        || !delivery
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return StatusCode::BAD_REQUEST;
    }
    // Unknown events are ignored; the worker reads current GitHub state.
    if !matches!(
        event,
        "pull_request"
            | "pull_request_review"
            | "check_run"
            | "check_suite"
            | "workflow_run"
            | "push"
            | "repository"
            | "repository_ruleset"
            | "branch_protection_rule"
            | "installation"
            | "installation_repositories"
            | "member"
            | "membership"
            | "organization"
            | "team"
            | "team_add"
    ) {
        return StatusCode::ACCEPTED;
    }
    let Ok(envelope) = serde_json::from_slice::<Envelope>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    if envelope.installation.id <= 0
        || envelope
            .repository
            .as_ref()
            .is_some_and(|repo| repo.id <= 0)
    {
        return StatusCode::BAD_REQUEST;
    }
    let installation_wide = matches!(
        event,
        "installation" | "installation_repositories" | "membership" | "organization" | "team"
    );
    let organization_rules = event == "repository_ruleset" && envelope.repository.is_none();
    let repository = if installation_wide || organization_rules {
        let Some((configured_installation, configured_repository)) = state.scope else {
            return StatusCode::BAD_REQUEST;
        };
        if configured_installation != envelope.installation.id {
            return StatusCode::ACCEPTED;
        }
        configured_repository
    } else if let Some(repo) = envelope.repository {
        repo.id
    } else {
        return StatusCode::BAD_REQUEST;
    };
    if state
        .scope
        .is_some_and(|scope| scope != (envelope.installation.id, repository))
    {
        return StatusCode::ACCEPTED;
    }
    // Event headers are not signed. Only bypass the admission wakeup when
    // the authenticated body also has the expected CI object and no obvious
    // PR, push, review, or security-event fields. Ambiguous bodies retain the
    // conservative full reconciliation path.
    let ci_only = state.scope.is_some()
        && ci_observation_event(event)
        && serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .is_some_and(|body| {
                body.as_object().is_some_and(|body| {
                    body.get(event).is_some_and(serde_json::Value::is_object)
                        && [
                            "pull_request",
                            "review",
                            "ref",
                            "rule",
                            "ruleset",
                            "member",
                            "team",
                        ]
                        .iter()
                        .all(|key| !body.contains_key(*key))
                })
            });
    let digest: [u8; 32] = Sha256::digest(&body).into();
    let delivery = delivery.to_owned();
    let event = event.to_owned();
    match tokio::task::spawn_blocking(move || {
        let mut inbox = Inbox::open(state.db)?;
        inbox.record_inner(
            &delivery,
            &event,
            &digest,
            envelope.installation.id,
            repository,
            ci_only,
        )
    })
    .await
    {
        Ok(Ok(Record::New | Record::Duplicate)) => StatusCode::ACCEPTED,
        Ok(Ok(Record::Collision)) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Record {
    New,
    Duplicate,
    Collision,
}

#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
}

/// These events concern CI observations, not PR admission. The observer polls
/// GitHub independently; each delivery still invalidates saved CI candidates
/// and advances the durable delivery clock.
fn ci_observation_event(event: &str) -> bool {
    matches!(event, "check_run" | "check_suite" | "workflow_run")
}

fn security_event(event: &str) -> bool {
    matches!(
        event,
        "repository"
            | "repository_ruleset"
            | "branch_protection_rule"
            | "installation"
            | "installation_repositories"
            | "member"
            | "membership"
            | "organization"
            | "team"
            | "team_add"
    )
}

#[derive(Debug, PartialEq, Eq)]
pub struct Reconciliation {
    pub installation: i64,
    pub repository: i64,
    pub revision: i64,
}

pub struct Inbox {
    conn: Connection,
}
impl Inbox {
    pub fn open(path: impl AsRef<std::path::Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        crate::database::configure_writer(&conn)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS webhook_deliveries (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            delivery_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL, digest BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS webhook_collisions (
            delivery_id TEXT NOT NULL, event TEXT NOT NULL, digest BLOB NOT NULL,
            installation INTEGER NOT NULL, repository INTEGER NOT NULL,
            PRIMARY KEY(delivery_id,event,digest,installation,repository)
        );
        CREATE TABLE IF NOT EXISTS reconciliations (
            installation INTEGER NOT NULL, repository INTEGER NOT NULL, revision INTEGER NOT NULL,
            PRIMARY KEY(installation, repository)
        );",
        )?;
        Ok(Self { conn })
    }

    /// Record a hint without a verified payload shape. Such hints always wake
    /// admission; revisions are never reused after acknowledgement.
    pub fn record(
        &mut self,
        delivery: &str,
        event: &str,
        digest: &[u8; 32],
        installation: i64,
        repository: i64,
    ) -> Result<Record, InboxError> {
        self.record_inner(delivery, event, digest, installation, repository, false)
    }

    fn record_inner(
        &mut self,
        delivery: &str,
        event: &str,
        digest: &[u8; 32],
        installation: i64,
        repository: i64,
        ci_only: bool,
    ) -> Result<Record, InboxError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<(String, Vec<u8>)> = tx
            .query_row(
                "SELECT event, digest FROM webhook_deliveries WHERE delivery_id=?1",
                [delivery],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((old_event, old_digest)) = previous {
            if old_event == event && old_digest == digest {
                return Ok(Record::Duplicate);
            }
            // A distinct signed body or event under an existing delivery ID
            // cannot be discarded as a duplicate. Journal it once and wake a
            // fresh reconciliation without replacing the original delivery.
            let inserted = tx.execute("INSERT OR IGNORE INTO webhook_collisions(delivery_id,event,digest,installation,repository) VALUES(?1,?2,?3,?4,?5)", params![delivery,event,digest.as_slice(),installation,repository])?;
            if inserted != 0 {
                tx.execute("INSERT INTO webhook_deliveries(delivery_id,event,digest) VALUES('collision-' || lower(hex(randomblob(16))), '__collision__', zeroblob(32))", [])?;
                let revision = tx.last_insert_rowid();
                tx.execute("INSERT INTO reconciliations(installation,repository,revision) VALUES(?1,?2,?3) ON CONFLICT(installation,repository) DO UPDATE SET revision=excluded.revision", params![installation,repository,revision])?;
                tx.execute(
                    "DELETE FROM webhook_deliveries WHERE sequence=?1",
                    [revision],
                )?;
                crate::store::invalidate_ci_candidate_hint(&tx, installation, repository)?;
                if security_event(event) {
                    crate::store::invalidate_security_event(&tx, installation, repository)?;
                }
            }
            tx.commit()?;
            return Ok(Record::Collision);
        }
        tx.execute(
            "INSERT INTO webhook_deliveries(delivery_id, event, digest) VALUES(?1, ?2, ?3)",
            params![delivery, event, digest.as_slice()],
        )?;
        let revision = tx.last_insert_rowid();
        if !ci_only {
            tx.execute(
                "INSERT INTO reconciliations(installation, repository, revision) VALUES(?1, ?2, ?3)
                ON CONFLICT(installation, repository) DO UPDATE SET revision=excluded.revision",
                params![installation, repository, revision],
            )?;
        }
        crate::store::invalidate_ci_candidate_hint(&tx, installation, repository)?;
        if security_event(event) {
            crate::store::invalidate_security_event(&tx, installation, repository)?;
        }
        tx.commit()?;
        Ok(Record::New)
    }
    pub fn pending(&self) -> rusqlite::Result<Vec<Reconciliation>> {
        let mut stmt = self.conn.prepare(
            "SELECT installation, repository, revision FROM reconciliations ORDER BY revision",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Reconciliation {
                installation: r.get(0)?,
                repository: r.get(1)?,
                revision: r.get(2)?,
            })
        })?;
        rows.collect()
    }
    pub fn pending_for(
        &self,
        installation: i64,
        repository: i64,
    ) -> rusqlite::Result<Option<Reconciliation>> {
        self.conn.query_row("SELECT installation,repository,revision FROM reconciliations WHERE installation=?1 AND repository=?2",params![installation,repository],|r| Ok(Reconciliation { installation:r.get(0)?, repository:r.get(1)?, revision:r.get(2)? })).optional()
    }
    /// Queue an internal full scan only if no scan is already pending. The
    /// AUTOINCREMENT clock is shared with webhook deliveries and never reused.
    pub fn schedule_if_idle(
        &mut self,
        installation: i64,
        repository: i64,
    ) -> rusqlite::Result<bool> {
        if installation <= 0 || repository <= 0 {
            return Err(rusqlite::Error::InvalidParameterName(
                "invalid repository scope".into(),
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM reconciliations WHERE installation=?1 AND repository=?2)",
            params![installation, repository],
            |r| r.get(0),
        )?;
        if exists {
            return Ok(false);
        }
        tx.execute("INSERT INTO webhook_deliveries(delivery_id,event,digest) VALUES('internal-' || lower(hex(randomblob(16))), '__scan__', zeroblob(32))", [])?;
        let revision = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO reconciliations(installation,repository,revision) VALUES(?1,?2,?3)",
            params![installation, repository, revision],
        )?;
        tx.execute(
            "DELETE FROM webhook_deliveries WHERE sequence=?1",
            [revision],
        )?;
        tx.commit()?;
        Ok(true)
    }
    /// A newer delivery prevents an older worker from deleting its wakeup.
    pub fn acknowledge(&self, job: &Reconciliation) -> rusqlite::Result<bool> {
        Ok(self.conn.execute(
            "DELETE FROM reconciliations WHERE installation=?1 AND repository=?2 AND revision=?3",
            params![job.installation, job.repository, job.revision],
        )? == 1)
    }
}

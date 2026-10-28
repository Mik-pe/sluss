//! Local end-to-end CI-only scenario: real Git objects and a simulated GitHub API.
use super::*;
use std::path::{Path as FsPath, PathBuf};

#[derive(Clone)]
struct Scenario {
    server: ServerState,
    remote: PathBuf,
    base: String,
    head: String,
}

fn git(repo: &FsPath, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

async fn app_token(
    State(s): State<Scenario>,
    path: Path<u64>,
    headers: HeaderMap,
    body: Json<Value>,
) -> impl axum::response::IntoResponse {
    token(State(s.server), path, headers, body).await
}
async fn repo(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    dispatch_repo(State(s.server), headers).await
}
async fn branch(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    let Json(mut value) = doctor_branch(State(s.server), headers).await;
    value["commit"]["sha"] = json!(s.base);
    Json(value)
}
async fn pulls(
    State(s): State<Scenario>,
    headers: HeaderMap,
    query: Query<BTreeMap<String, String>>,
) -> Json<Value> {
    open_pulls(State(s.server), headers, query).await
}
async fn pull_detail(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    let Json(mut value) = pull(State(s.server), headers).await;
    value["head"]["sha"] = json!(s.head);
    value["base"]["sha"] = json!(s.base);
    Json(value)
}
async fn pull_reviews(
    State(s): State<Scenario>,
    headers: HeaderMap,
    query: Query<BTreeMap<String, String>>,
) -> Json<Value> {
    let Json(mut value) = reviews(State(s.server), headers, query).await;
    for review in value.as_array_mut().unwrap() {
        review["commit_id"] = json!(s.head);
    }
    Json(value)
}
async fn collaborator(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    dispatch_permission(State(s.server), headers).await
}
async fn graphql(State(s): State<Scenario>, headers: HeaderMap, body: Json<Value>) -> Json<Value> {
    let Json(mut value) = graphql_schema(State(s.server), headers, body).await;
    if let Some(pr) = value.pointer_mut("/data/repository/pullRequest") {
        pr["baseRefOid"] = json!(s.base);
        pr["headRefOid"] = json!(s.head);
    }
    Json(value)
}
async fn runners(
    State(s): State<Scenario>,
    headers: HeaderMap,
    query: Query<BTreeMap<String, String>>,
) -> Json<Value> {
    repository_runners(State(s.server), headers, query).await
}
async fn workflow(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    doctor_workflow(State(s.server), headers).await
}
async fn rules(
    State(s): State<Scenario>,
    headers: HeaderMap,
    query: Query<BTreeMap<String, String>>,
) -> Json<Value> {
    doctor_rules(State(s.server), headers, query).await
}
async fn protection(
    State(s): State<Scenario>,
    headers: HeaderMap,
) -> impl axum::response::IntoResponse {
    doctor_protection(State(s.server), headers).await
}
async fn ruleset(
    State(s): State<Scenario>,
    headers: HeaderMap,
) -> impl axum::response::IntoResponse {
    doctor_ruleset(State(s.server), headers).await
}
async fn staging(State(s): State<Scenario>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let reference = "refs/heads/sluss/batch-0123456789abcdef";
    Json(
        json!({"ref":reference,"object":{"sha":git(&s.remote, &["rev-parse", reference]),"type":"commit"}}),
    )
}
async fn tree(
    State(s): State<Scenario>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let output = git(&s.remote, &["ls-tree", &id]);
    let entries: Vec<Value> = output.lines().map(|line| {
        let (meta, name) = line.split_once('\t').unwrap();
        let mut fields = meta.split_whitespace();
        json!({"mode":fields.next().unwrap(),"type":fields.next().unwrap(),"sha":fields.next().unwrap(),"path":name})
    }).collect();
    Json(json!({"sha":id,"truncated":false,"tree":entries}))
}
async fn blob(
    State(s): State<Scenario>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Json<Value> {
    use base64::Engine;
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let output = Command::new("git")
        .arg("-C")
        .arg(&s.remote)
        .args(["cat-file", "blob", &id])
        .output()
        .unwrap();
    assert!(output.status.success());
    let contents = output.stdout;
    Json(
        json!({"sha":id,"size":contents.len(),"encoding":"base64","content":base64::engine::general_purpose::STANDARD.encode(contents)}),
    )
}
async fn send(
    State(s): State<Scenario>,
    headers: HeaderMap,
    body: Json<Value>,
) -> impl axum::response::IntoResponse {
    dispatch_post(State(s.server), headers, body).await
}
async fn run(
    State(s): State<Scenario>,
    headers: HeaderMap,
    uri: axum::extract::OriginalUri,
) -> Json<Value> {
    let Json(mut value) = dispatched_run(State(s.server), headers, uri).await;
    value["head_sha"] = json!(git(
        &s.remote,
        &["rev-parse", "refs/heads/sluss/batch-0123456789abcdef"]
    ));
    Json(value)
}
async fn jobs(
    State(s): State<Scenario>,
    headers: HeaderMap,
    query: Query<BTreeMap<String, String>>,
    uri: axum::extract::OriginalUri,
) -> Json<Value> {
    let Json(mut value) = actions_jobs(State(s.server), headers, query, uri).await;
    for job in value["jobs"].as_array_mut().unwrap() {
        job["head_sha"] = json!(git(
            &s.remote,
            &["rev-parse", "refs/heads/sluss/batch-0123456789abcdef"]
        ));
    }
    Json(value)
}

#[tokio::test]
async fn real_staging_commit_survives_dispatch_observation_and_restart() {
    use sluss::{
        batch::State as BatchState,
        coordinator::prepare_local_from_remote,
        scheduler::{Capacity, Scheduler},
        store::Store,
        webhook::Inbox,
    };
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "-b", "main"]);
    std::fs::create_dir_all(source.path().join(".github/workflows")).unwrap();
    std::fs::write(
        source.path().join(".github/workflows/ci.yml"),
        WORKFLOW_GOOD,
    )
    .unwrap();
    std::fs::write(source.path().join("base"), "base\n").unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "base"],
    );
    let base = git(source.path(), &["rev-parse", "HEAD"]);
    git(source.path(), &["checkout", "-b", "feature"]);
    std::fs::write(source.path().join("change"), "change\n").unwrap();
    git(source.path(), &["add", "."]);
    git(
        source.path(),
        &["-c", "commit.gpgsign=false", "commit", "-m", "feature"],
    );
    let head = git(source.path(), &["rev-parse", "HEAD"]);
    let remote = tempfile::tempdir().unwrap();
    git(remote.path(), &["init", "--bare"]);
    git(
        source.path(),
        &[
            "push",
            remote.path().to_str().unwrap(),
            "main:main",
            "feature:refs/pull/19/head",
        ],
    );

    let (_keys, private, public) = keys();
    let posts = Arc::new(AtomicUsize::new(0));
    let s = Scenario {
        server: ServerState {
            public_key: Arc::new(public),
            mode: Arc::new(AtomicU8::new(0)),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: posts.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        },
        remote: remote.path().into(),
        base: base.clone(),
        head: head.clone(),
    };
    let router = Router::new()
        .route("/app/installations/{id}/access_tokens", post(app_token))
        .route("/graphql", post(graphql))
        .route("/repos/owner/repo", get(repo))
        .route("/repos/owner/repo/branches/main", get(branch))
        .route("/repos/owner/repo/pulls", get(pulls))
        .route("/repos/owner/repo/pulls/19", get(pull_detail))
        .route("/repos/owner/repo/pulls/19/reviews", get(pull_reviews))
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(collaborator),
        )
        .route("/repos/owner/repo/actions/runners", get(runners))
        .route("/repos/owner/repo/actions/workflows/17", get(workflow))
        .route("/repos/owner/repo/rules/branches/main", get(rules))
        .route(
            "/repos/owner/repo/branches/main/protection",
            get(protection),
        )
        .route("/repos/owner/repo/rulesets/10", get(ruleset))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(staging),
        )
        .route("/repos/owner/repo/git/trees/{id}", get(tree))
        .route("/repos/owner/repo/git/blobs/{id}", get(blob))
        .route(
            "/repos/owner/repo/actions/workflows/17/dispatches",
            post(send),
        )
        .route("/repos/owner/repo/actions/runs/123", get(run))
        .route("/repos/owner/repo/actions/runs/123/attempts/1", get(run))
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1/jobs",
            get(jobs),
        )
        .with_state(s);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let config = data.path().join("sluss.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    let policy = sluss::config::Config::load(&config).unwrap().policy();
    let app = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = app.read_token(42, 7).await.unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut store = Store::open(&db).unwrap();
    sluss::reconcile::reconcile_once(&app, &token, &mut store, &event, &policy)
        .await
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    let prepared = prepare_local_from_remote(
        &mut store,
        id,
        remote.path(),
        "main",
        &[".github/workflows/ci.yml".into()],
    )
    .unwrap();
    assert_eq!(
        git(remote.path(), &["rev-parse", &prepared.reference]),
        prepared.sha
    );
    assert_eq!(
        git(
            remote.path(),
            &["show", &format!("{}:change", prepared.sha)]
        ),
        "change"
    );
    drop(store);
    drop(inbox);
    let capacity: Capacity = toml::from_str("[pools]\nrust=1\n[hosts.sluss-host-test]\nslots=1\nrunner_ids=[1]\n[hosts.sluss-host-test.pools]\nrust=1\n").unwrap();
    let settings = sluss::dispatch::Settings {
        workflow_id: 17,
        required_jobs: ["job-1-0".into()].into(),
        request: sluss::scheduler::Request {
            pools: [("rust".into(), 1)].into(),
        },
        verification_policy: None,
    };
    assert!(matches!(
        sluss::dispatch::batch(&db, &app, &policy, &capacity, &settings, id)
            .await
            .unwrap(),
        sluss::dispatch::Outcome::Sent { run: 123, .. }
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let scheduler = Scheduler::open(&db, capacity.clone()).unwrap();
    let pending = scheduler.pending_dispatches().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].plan.integration_sha, prepared.sha);
    assert_eq!(
        scheduler.receipt_source(id).unwrap().as_deref(),
        Some("direct")
    );
    drop(scheduler);
    drop(app);
    // Reconstruct the application as well as the database handles after dispatch.
    let app = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    assert_eq!(
        sluss::dispatch::batch(&db, &app, &policy, &capacity, &settings, id)
            .await
            .unwrap(),
        sluss::dispatch::Outcome::AlreadyStarted
    );
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    sluss::worker::observe_dispatches_with_completion(&app, &db, &capacity, &policy, true)
        .await
        .unwrap();
    let reopened = Store::open(&db).unwrap();
    assert_eq!(
        reopened.batch(id).unwrap().unwrap().0.state,
        BatchState::CiComplete {
            integration_sha: prepared.sha,
            run_id: 123,
            conclusion: "success".into()
        }
    );
    let metrics = reopened.metrics().unwrap();
    assert_eq!(metrics.completed_jobs, 1);
    assert_eq!(metrics.runner_milliseconds, 60_000);
    sluss::worker::observe_dispatches_with_completion(&app, &db, &capacity, &policy, true)
        .await
        .unwrap();
    assert_eq!(Store::open(&db).unwrap().metrics().unwrap(), metrics);
    assert_eq!(git(remote.path(), &["rev-parse", "refs/heads/main"]), base);
    server.abort();
}

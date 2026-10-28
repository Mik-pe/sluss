use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use serde_json::{Value, json};
use sluss::github::{App, GitHubError};
use std::{
    collections::BTreeMap,
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

static PENDING_CHECK_STUB: OnceLock<Mutex<Option<Value>>> = OnceLock::new();
fn pending_check_stub() -> &'static Mutex<Option<Value>> {
    PENDING_CHECK_STUB.get_or_init(|| Mutex::new(None))
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    iat: u64,
    exp: u64,
}
#[derive(Clone)]
struct ServerState {
    public_key: Arc<Vec<u8>>,
    mode: Arc<AtomicU8>,
    pulls: Arc<AtomicUsize>,
    permissions: Arc<AtomicUsize>,
    token_calls: Arc<AtomicUsize>,
}
async fn permission(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.permissions.fetch_add(1, Ordering::SeqCst);
    let access = if mode == 7 || (mode == 9 && count > 0) {
        "read"
    } else if mode == 25 || mode == 26 {
        "admin"
    } else {
        "write"
    };
    let id = if mode == 8 { 10 } else { 9 };
    Json(json!({"permission":access,"user":{"id":id,"login":"reviewer","type":"User"}}))
}
async fn dispatch_permission(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let permission = if state.mode.load(Ordering::SeqCst) == 38 {
        "read"
    } else {
        "write"
    };
    Json(json!({"permission":permission,"user":{"id":9,"login":"reviewer","type":"User"}}))
}
async fn pull(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let sha = if state.mode.load(Ordering::SeqCst) == 5 && count > 0 {
        "c".repeat(40)
    } else {
        "a".repeat(40)
    };
    let label = if state.mode.load(Ordering::SeqCst) == 10 {
        "do-not-merge"
    } else {
        "needs-work"
    };
    let base = if state.mode.load(Ordering::SeqCst) == 36 {
        "c"
    } else {
        "b"
    };
    Json(
        json!({"number":19,"state":"open","draft":false,"user":{"id":4,"login":"author","type":"User"},"head":{"sha":sha,"ref":if state.mode.load(Ordering::SeqCst)==232 {"main"} else {"feature/19"},"repo":if state.mode.load(Ordering::SeqCst)==231 {Value::Null} else {json!({"id":if state.mode.load(Ordering::SeqCst)==230 {8} else {7}})}},"base":{"sha":base.repeat(40),"ref":"main","repo":{"id":7}},"labels":[{"name":label}]}),
    )
}
async fn reviews(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("per_page").map(String::as_str), Some("100"));
    let page = query["page"].parse::<u64>().unwrap();
    let count = if state.mode.load(Ordering::SeqCst) == 6 {
        if page == 1 { 100 } else { 1 }
    } else {
        1
    };
    let mut items: Vec<Value> = (0..count).map(|n| json!({"id":(page-1)*100+n+1,"user":{"id":9,"login":"reviewer","type":"User"},"state":"APPROVED","commit_id":"a".repeat(40),"submitted_at":"2026-01-01T00:00:00Z"})).collect();
    if matches!(state.mode.load(Ordering::SeqCst), 24 | 26) {
        items.push(json!({"id":2,"user":{"id":9,"login":"reviewer","type":"User"},"state":"COMMENTED","commit_id":"a".repeat(40),"submitted_at":"2026-01-02T00:00:00Z"}));
    }
    Json(json!(items))
}
async fn open_pulls(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("page").map(String::as_str), Some("1"));
    if matches!(state.mode.load(Ordering::SeqCst), 46 | 47) {
        Json(json!([{"number":19}, {"number":20}]))
    } else {
        Json(json!([{"number":19}]))
    }
}
async fn pull20(state: State<ServerState>, headers: HeaderMap) -> Json<Value> {
    let Json(mut value) = pull(state, headers).await;
    value["number"] = json!(20);
    Json(value)
}
async fn actions_run(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let sha = if mode == 12 && count > 0 { "b" } else { "a" };
    Json(
        json!({"id":123,"run_attempt":2,"workflow_id":17,"head_sha":sha.repeat(40),"head_branch":"sluss/test","path":".github/workflows/ci.yml@refs/heads/sluss/test","event":"push","status":"completed","conclusion":"success","repository":{"id":if mode == 11 { 8 } else { 7 }},"head_repository":{"id":7}}),
    )
}
async fn actions_jobs(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    uri: axum::extract::OriginalUri,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("per_page").map(String::as_str), Some("100"));
    let page: u64 = query["page"].parse().unwrap();
    let mode = state.mode.load(Ordering::SeqCst);
    let count = if mode == 43 {
        0
    } else if mode == 15 && page == 1 {
        100
    } else {
        1
    };
    let total = if mode == 43 {
        0
    } else if mode == 15 {
        101
    } else if mode == 14 {
        2
    } else {
        1
    };
    let jobs: Vec<Value> = (0..count).map(|n| json!({"id":(page-1)*100+n+1,"run_id":if mode == 13 || uri.0.path().contains("/runs/124") { 124 } else { 123 },"head_sha":"a".repeat(40),"name":format!("job-{page}-{n}"),"status":"completed","conclusion":if matches!(mode,40 | 95..=100 | 103 | 129) { "failure" } else { "success" },"started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T00:01:00Z","runner_id":1})).collect();
    Json(json!({"total_count":total,"jobs":jobs}))
}
async fn dispatch_repo(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let node = if state.mode.load(Ordering::SeqCst) == 63 {
        "R_other"
    } else {
        "R_repo"
    };
    Json(json!({"id": 7, "node_id": node}))
}
async fn dispatch_ref(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let sha = if state.mode.load(Ordering::SeqCst) == 16 {
        "b"
    } else {
        "a"
    };
    Json(
        json!({"ref":"refs/heads/sluss/batch-0123456789abcdef","object":{"sha":sha.repeat(40),"type":"commit"}}),
    )
}
async fn dispatched_run(
    State(state): State<ServerState>,
    headers: HeaderMap,
    path: axum::extract::OriginalUri,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let latest = !path.0.path().contains("/attempts/");
    let branch = if mode == 18 {
        "sluss/other"
    } else {
        "sluss/batch-0123456789abcdef"
    };
    let event = if mode == 20 {
        "push"
    } else {
        "workflow_dispatch"
    };
    let attempt = if latest && (mode == 21 || (mode == 42 && count >= 6)) {
        2
    } else {
        1
    };
    let workflow_id = if mode == 23 || (mode == 104 && path.0.path().contains("/runs/124")) {
        18
    } else {
        17
    };
    let workflow_path = if mode == 51 {
        ".github/workflows/other.yml@refs/heads/sluss/batch-0123456789abcdef"
    } else if mode == 239 {
        ".github/workflows/ci.yml@refs/heads/main"
    } else {
        ".github/workflows/ci.yml@refs/heads/sluss/batch-0123456789abcdef"
    };
    let sha = if mode == 22 && count >= 3 { "b" } else { "a" };
    let status = if matches!(mode, 31 | 32) {
        "in_progress"
    } else {
        "completed"
    };
    let conclusion = if matches!(mode, 31 | 32) {
        None
    } else if matches!(mode, 40 | 95..=100 | 103 | 129) {
        Some("failure")
    } else if mode == 43 {
        Some("cancelled")
    } else {
        Some("success")
    };
    Json(
        json!({"id":if path.0.path().contains("/runs/124") {124} else {123},"run_attempt":attempt,"workflow_id":workflow_id,"head_sha":sha.repeat(40),"head_branch":branch,"path":workflow_path,"event":event,"status":status,"conclusion":conclusion,"repository":{"id":7},"head_repository":{"id":if mode == 19 { 8 } else { 7 }}}),
    )
}

async fn discovery_runs(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(
        query.get("branch").map(String::as_str),
        Some("sluss/batch-0123456789abcdef")
    );
    assert_eq!(
        query.get("event").map(String::as_str),
        Some("workflow_dispatch")
    );
    assert_eq!(query.get("head_sha"), Some(&"a".repeat(40)));
    assert_eq!(query.get("per_page").map(String::as_str), Some("100"));
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    let run = |id: u64, attempt: u32, branch: &str, path: &str| json!({"id":id,"run_attempt":attempt,"workflow_id":17,"head_sha":"a".repeat(40),"head_branch":branch,"path":path,"event":"workflow_dispatch","status":"completed","conclusion":"success","repository":{"id":7},"head_repository":{"id":7}});
    if mode == 225 {
        let page = query.get("page").unwrap();
        let rows: Vec<_> = if page == "1" {
            (1..=100)
                .map(|id| run(id, 1, "sluss/other", ".github/workflows/ci.yml"))
                .collect()
        } else {
            assert_eq!(page, "2");
            vec![run(
                101,
                1,
                "sluss/batch-0123456789abcdef",
                ".github/workflows/ci.yml",
            )]
        };
        return Json(json!({"total_count":101,"workflow_runs":rows}));
    }
    assert_eq!(query.get("page").map(String::as_str), Some("1"));
    if mode == 223 {
        return Json(json!({"total_count":0,"workflow_runs":[]}));
    }
    if mode == 224 {
        let mut item = run(
            123,
            1,
            "sluss/batch-0123456789abcdef",
            ".github/workflows/ci.yml",
        );
        if count % 2 == 0 {
            item["status"] = json!("in_progress");
            item["conclusion"] = Value::Null;
        }
        return Json(json!({"total_count":1,"workflow_runs":[item]}));
    }
    if mode == 222 {
        return Json(json!({"total_count":1001,"workflow_runs":[]}));
    }
    if mode == 220 {
        return Json(
            json!({"total_count":2,"workflow_runs":[run(123,1,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml"),run(123,1,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml")]}),
        );
    }
    if mode == 221 && count % 2 == 1 {
        return Json(
            json!({"total_count":1,"workflow_runs":[run(124,1,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml")]}),
        );
    }
    if mode == 221 {
        return Json(
            json!({"total_count":1,"workflow_runs":[run(123,1,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml")]}),
        );
    }
    Json(json!({"total_count":4,"workflow_runs":[
        run(123,1,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml"),
        run(125,2,"sluss/batch-0123456789abcdef",".github/workflows/ci.yml"),
        run(126,1,"sluss/other",".github/workflows/ci.yml"),
        run(127,1,"sluss/batch-0123456789abcdef",".github/workflows/other.yml")]}))
}
async fn cancel_run(State(state): State<ServerState>, headers: HeaderMap) -> StatusCode {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    state.permissions.fetch_add(1, Ordering::SeqCst);
    if state.mode.load(Ordering::SeqCst) == 32 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::ACCEPTED
    }
}
async fn reconcile_branch(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let sha = if mode == 34
        || mode == 36
        || (mode == 35 && state.pulls.fetch_add(1, Ordering::SeqCst) > 0)
    {
        "c"
    } else {
        "b"
    };
    Json(json!({"name":"main","protected":true,"commit":{"sha":sha.repeat(40)}}))
}
async fn doctor_branch(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    let sha = if (mode == 30 && count > 0) || (mode == 37 && count >= 8) {
        "c"
    } else {
        "b"
    };
    Json(json!({"name":"main","protected":true,"commit":{"sha":sha.repeat(40)}}))
}
async fn graphql_schema(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let query = body["query"].as_str().unwrap();
    if query.contains("reviewDecision") {
        assert!(query.starts_with("query("));
        assert_eq!(
            body["variables"],
            json!({"owner":"owner","name":"repo","number":19})
        );
        let mode = state.mode.load(Ordering::SeqCst);
        if mode == 109 {
            return Json(json!({"data":null,"errors":[{"message":"unavailable"}]}));
        }
        let count = state.token_calls.fetch_add(1, Ordering::SeqCst);
        if mode == 121 && count == 0 {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while state.mode.load(Ordering::SeqCst) == 121 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("preview race test must release provider read");
        }
        let decision = match mode {
            104 => Some("REVIEW_REQUIRED"),
            105 => Some("CHANGES_REQUESTED"),
            106 => None,
            119 => Some("UNKNOWN_NEW_STATUS"),
            108 if count % 2 == 1 => Some("REVIEW_REQUIRED"),
            117 if count >= 2 => Some("REVIEW_REQUIRED"),
            _ => Some("APPROVED"),
        };
        return Json(
            json!({"data":{"repository":{"id":if mode==110 {"R_other"} else {"R_repo"},"pullRequest":{
            "number":if mode==114 {20} else {19},"state":if mode==115 {"CLOSED"} else {"OPEN"},"isDraft":mode==113,
            "baseRefName":if mode==120 {"other"} else {"main"},"baseRefOid":if mode==112 {"c".repeat(40)} else {"b".repeat(40)},
            "headRefOid":if mode==111 {"c".repeat(40)} else {"a".repeat(40)},"reviewDecision":decision}}}}),
        );
    }
    assert!(query.starts_with("query {"));
    assert!(query.contains("__schema"));
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 61 {
        return Json(json!({"errors":[{"message":"introspection disabled"}]}));
    }
    let mut fields = vec![
        json!({"name":"name"}),
        json!({"name":"afterOid"}),
        json!({"name":"force"}),
    ];
    if mode != 60 {
        fields.push(json!({"name":"beforeOid"}));
    }
    if mode == 62 {
        fields.push(json!({"name":"beforeOid"}));
    }
    Json(
        json!({"data":{"__schema":{"mutationType":{"fields":[{"name":"updateRefs"}]}},"refUpdate":{"inputFields":fields},"updateInput":{"inputFields":[{"name":"repositoryId"},{"name":"refUpdates"},{"name":"clientMutationId"}]}}}),
    )
}
async fn doctor_workflow(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let path = if mode == 48 || (mode == 50 && state.pulls.fetch_add(1, Ordering::SeqCst) % 2 == 1)
    {
        ".github/workflows/other.yml"
    } else {
        ".github/workflows/ci.yml"
    };
    let status = if mode == 49 {
        "disabled_manually"
    } else {
        "active"
    };
    Json(json!({"id":17,"path":path,"state":status}))
}
async fn doctor_rules(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query["page"], "1");
    let mode = state.mode.load(Ordering::SeqCst);
    let different = mode == 64
        || (matches!(mode, 65 | 66) && state.token_calls.fetch_add(1, Ordering::SeqCst) % 2 == 1);
    Json(
        json!([{"type":if different {"required_linear_history"} else {"pull_request"},"ruleset_id":10,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"},{"type":"update","parameters":{"update_allows_fetch_and_merge":false},"ruleset_id":10,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"}]),
    )
}
async fn doctor_protection(
    State(state): State<ServerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    if state.mode.load(Ordering::SeqCst) == 69 {
        return (StatusCode::NOT_FOUND, Json(json!({"message":"not found"})));
    }
    (
        StatusCode::OK,
        Json(
            json!({"enforce_admins":{"enabled":true},"allow_force_pushes":{"enabled":state.mode.load(Ordering::SeqCst) == 76},"allow_deletions":{"enabled":false},"required_linear_history":{"enabled":false},"required_status_checks":null,"required_pull_request_reviews":null,"restrictions":null}),
        ),
    )
}
async fn doctor_ruleset(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let mut body = json!({"id":10,"target":"branch","enforcement":"active","source_type":"Repository","source":if mode == 74 {"owner/other"} else {"owner/repo"},"rules":[{"type":if mode == 71 {"deletion"} else {"pull_request"}},{"type":"update","parameters":{"update_allows_fetch_and_merge":false}}],"bypass_actors":[{"actor_type":"Integration","actor_id":17,"bypass_mode":"always"}]});
    if mode == 67 {
        body.as_object_mut().unwrap().remove("bypass_actors");
    }
    if mode == 68 {
        body["description"] = json!(state.token_calls.fetch_add(1, Ordering::SeqCst) % 2);
    }
    if mode == 72 {
        body["bypass_actors"] =
            json!([{"actor_type":"Integration","actor_id":null,"bypass_mode":"always"}]);
    }
    if mode == 73 || (125..=127).contains(&mode) || matches!(mode, 132..=134 | 138) {
        body["bypass_actors"] = json!([]);
    }
    if mode == 77 {
        body["bypass_actors"][0]["actor_id"] = json!(18);
    }
    Json(body)
}
async fn authenticated_app(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    let jwt = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    let claims = decode::<Claims>(
        jwt,
        &DecodingKey::from_rsa_pem(&state.public_key).unwrap(),
        &Validation::new(Algorithm::RS256),
    )
    .unwrap()
    .claims;
    assert_eq!(claims.iss, "app-client-id");
    let mode = state.mode.load(Ordering::SeqCst);
    let id = if mode == 80 {
        0
    } else if mode == 81 && state.token_calls.fetch_add(1, Ordering::SeqCst) % 2 == 1 {
        18
    } else {
        17
    };
    Json(
        json!({"id":id,"client_id":if mode == 82 {"wrong"} else {"app-client-id"},"slug":"sluss-test","node_id":"A_app"}),
    )
}
async fn verified_check(State(state): State<ServerState>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let changed = mode == 90 && state.token_calls.fetch_add(1, Ordering::SeqCst) % 2 == 1;
    Json(json!({
        "id":if mode == 91 {334} else {333},
        "name":if mode == 86 || changed {"sluss/other"} else {"sluss/verified"},
        "head_sha":if mode == 85 {"b".repeat(40)} else {"a".repeat(40)},
        "external_id":if mode == 87 {"batch-other"} else {"batch-0123456789abcdef"},
        "status":if mode == 89 {"in_progress"} else {"completed"},
        "conclusion":if mode == 88 {"neutral"} else {"success"},
        "app":{"id":if mode == 84 {18} else {17},"node_id":if mode == 93 {"A_other"} else {"A_app"}},
        "check_suite":{"id":if mode == 92 {0} else {44}}
    }))
}
async fn failed_check_post(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    if body["status"] == "in_progress" {
        return pending_check_post(state, body).await;
    }
    assert_eq!(body["name"], "sluss/verified");
    assert_eq!(body["head_sha"], "a".repeat(40));
    assert_eq!(body["external_id"], "batch-0123456789abcdef");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["conclusion"], "failure");
    assert!(
        body["output"]["summary"]
            .as_str()
            .unwrap()
            .contains("does not authorize")
    );
    assert!(
        body["output"]["summary"]
            .as_str()
            .unwrap()
            .contains("run 123")
    );
    state.permissions.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    if matches!(mode, 99 | 103) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.mode.load(Ordering::SeqCst) == mode {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("race test must release sender");
    }
    if mode == 95 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"message":"ambiguous"})),
        );
    }
    (
        StatusCode::CREATED,
        Json(
            json!({"id":if mode==99 {445} else {444},"name":"sluss/verified","head_sha":"a".repeat(40),"external_id":"batch-0123456789abcdef","status":"completed","conclusion":if mode==97 {"neutral"} else {"failure"},"app":{"id":if mode==96 {18} else {17},"node_id":"A_app"},"check_suite":{"id":45}}),
        ),
    )
}
async fn pending_check_post(state: ServerState, body: Value) -> (StatusCode, Json<Value>) {
    assert_eq!(body["name"], "sluss/verified");
    assert_eq!(body["head_sha"], "a".repeat(40));
    assert_eq!(body["external_id"], "batch-0123456789abcdef");
    assert_eq!(body["status"], "in_progress");
    assert!(body.get("conclusion").is_none());
    assert!(
        body["output"]["summary"]
            .as_str()
            .unwrap()
            .contains("does not authorize a merge")
    );
    let mode = state.mode.load(Ordering::SeqCst);
    let candidate_id = if mode == 125 { 555 } else { 556 };
    let value = json!({"id":candidate_id,"name":body["name"],"head_sha":body["head_sha"],"external_id":body["external_id"],"status":"in_progress","conclusion":null,"app":{"id":17,"node_id":"A_app"},"check_suite":{"id":44},"output":body["output"]});
    *pending_check_stub().lock().unwrap() = Some(value.clone());
    state.permissions.fetch_add(1, Ordering::SeqCst);
    if mode == 126 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"ambiguous"})),
        );
    }
    if matches!(mode, 125 | 127 | 132 | 133 | 134) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.mode.load(Ordering::SeqCst) == mode {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pending-check race must release sender");
    }
    let mut response = value;
    if mode == 127 {
        response["id"] = json!(557);
    }
    if mode == 132 {
        response["check_suite"]["id"] = json!(99);
    }
    (StatusCode::CREATED, Json(response))
}

async fn cancel_pending_check_patch(
    State(state): State<ServerState>,
    Path(check_id): Path<u64>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["conclusion"], "cancelled");
    assert_eq!(body["output"]["title"], "Staging CI verification cancelled");
    assert!(
        body["output"]["summary"]
            .as_str()
            .unwrap()
            .contains("does not authorize a merge")
    );
    let mut value = pending_check_stub().lock().unwrap().clone().unwrap();
    assert_eq!(value["id"], check_id);
    value["status"] = body["status"].clone();
    value["conclusion"] = body["conclusion"].clone();
    value["output"] = body["output"].clone();
    *pending_check_stub().lock().unwrap() = Some(value.clone());
    state.permissions.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 135 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"ambiguous"})),
        );
    }
    if mode == 136 {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.mode.load(Ordering::SeqCst) == 136 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancellation race must release sender");
    }
    if mode == 137 {
        value["conclusion"] = json!("success");
    }
    (StatusCode::OK, Json(value))
}

async fn failed_check_get(
    State(state): State<ServerState>,
    Path(check_id): Path<u64>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    if check_id >= 555 {
        if mode == 138 {
            state.pulls.fetch_add(1, Ordering::SeqCst);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while state.mode.load(Ordering::SeqCst) == 138 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("provider-read race must release reader");
        }
        let stored = pending_check_stub().lock().unwrap().clone();
        let mut value = stored.unwrap_or_else(|| json!({"id":check_id,"name":"unknown"}));
        if value["id"].as_u64() != Some(check_id) {
            value["name"] = json!("unknown");
            value["id"] = json!(check_id);
        }
        if mode == 130 {
            value["status"] = json!("completed");
            value["conclusion"] = json!("success");
        }
        if mode == 131 {
            value["check_suite"]["id"] = json!(99);
        }
        return Json(value);
    }
    Json(
        json!({"id":check_id,"name":"sluss/verified","head_sha":if mode==101 {"b".repeat(40)} else {"a".repeat(40)},"external_id":"batch-0123456789abcdef","status":"completed","conclusion":if mode==97 {"neutral"} else {"failure"},"app":{"id":if mode==96 {18} else {17},"node_id":"A_app"},"check_suite":{"id":45},"output":{"title":"Staging CI failed","summary":format!("Recorded CI run {} failed. This check is not an attestation and does not authorize a merge.",if mode==102 {124} else {123})}}),
    )
}
async fn repository_runners(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query["page"], "1");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.pulls.fetch_add(1, Ordering::SeqCst);
    if matches!(mode, 44..=47 | 66) {
        return Json(json!({"total_count":2,"runners":[
            {"id":2,"name":"busy-runner","status":"online","busy":true,"labels":[{"name":"self-hosted"},{"name":"sluss-host-busy"}]},
            {"id":1,"name":"runner","status":"online","busy":mode == 45 || mode == 47,"labels":[{"name":"self-hosted"},{"name":"sluss-host-test"}]}
        ]}));
    }
    let id = if mode == 28 || (mode == 29 && count % 2 == 1) {
        2
    } else {
        1
    };
    Json(
        json!({"total_count":1,"runners":[{"id":id,"name":"runner","status":"online","busy":mode == 41,"labels":[{"name":"self-hosted"},{"name":"sluss-host-test"}]}]}),
    )
}
async fn dispatch_post(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(headers["x-github-api-version"], "2026-03-10");
    let suites = if state.mode.load(Ordering::SeqCst) == 33 {
        "api-tests,ui-tests"
    } else {
        "full"
    };
    assert_eq!(
        body,
        json!({"ref":"refs/heads/sluss/batch-0123456789abcdef", "return_run_details":true, "inputs":{"sluss_host":"sluss-host-test","sluss_suites":suites}})
    );
    state.permissions.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 123 {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while state.mode.load(Ordering::SeqCst) == 123 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dispatch race must release sender");
    }
    if mode == 17 {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})))
    } else if mode == 39 {
        (StatusCode::NO_CONTENT, Json(json!({})))
    } else {
        (
            StatusCode::OK,
            Json(json!({"workflow_run_id":if mode == 123 {124} else {123}})),
        )
    }
}
async fn token(
    State(state): State<ServerState>,
    Path(id): Path<u64>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.token_calls.fetch_add(1, Ordering::SeqCst);
    assert_eq!(id, 42);
    assert_eq!(body["repository_ids"], json!([7]));
    let write = body["permissions"]["actions"] == "write";
    let staging = body["permissions"]["contents"] == "write";
    let inventory = body["permissions"]["administration"] == "read";
    let checks_write = body["permissions"]["checks"] == "write";
    if inventory {
        assert_eq!(body["permissions"], json!({"administration":"read"}));
    } else if checks_write {
        assert_eq!(body["permissions"], json!({"checks":"write"}));
    } else if staging {
        assert_eq!(body["permissions"], json!({"contents":"write"}));
    } else if write {
        assert_eq!(
            body["permissions"],
            json!({"actions":"write","contents":"read"})
        );
    } else {
        assert_eq!(
            body["permissions"],
            json!({"actions":"read","checks":"read","contents":"read","pull_requests":"read"})
        );
    }
    let jwt = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap();
    let validation = Validation::new(Algorithm::RS256);
    let claims = decode::<Claims>(
        jwt,
        &DecodingKey::from_rsa_pem(&state.public_key).unwrap(),
        &validation,
    )
    .unwrap()
    .claims;
    let now = time::OffsetDateTime::now_utc().unix_timestamp() as u64;
    assert!(matches!(
        claims.iss.as_str(),
        "app-client-id" | "different-app"
    ));
    assert!(claims.iat <= now && now - claims.iat <= 65);
    assert!(claims.exp > now && claims.exp - now <= 540);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 1 {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(json!({"error":"do not follow"})),
        );
    }
    let expiry = if mode == 2 {
        time::OffsetDateTime::now_utc() - time::Duration::hours(1)
    } else {
        time::OffsetDateTime::now_utc() + time::Duration::minutes(59)
    };
    let mut permissions = BTreeMap::from([
        ("actions", "read"),
        ("checks", "read"),
        ("contents", "read"),
        ("pull_requests", "read"),
        ("metadata", "read"),
    ]);
    if write {
        permissions = BTreeMap::from([
            ("actions", "write"),
            ("contents", "read"),
            ("metadata", "read"),
        ]);
    }
    if staging {
        permissions = BTreeMap::from([("contents", "write"), ("metadata", "read")]);
    }
    if inventory {
        permissions = BTreeMap::from([("administration", "read"), ("metadata", "read")]);
    }
    if checks_write {
        permissions = BTreeMap::from([("checks", "write"), ("metadata", "read")]);
    }
    if mode == 3 {
        permissions.insert("contents", "write");
    }
    if mode == 27 {
        permissions.insert("actions", "write");
    }
    if mode == 226 {
        permissions.insert("issues", "read");
    }
    if mode == 227 {
        permissions.insert("metadata", "write");
    }
    let repo = if mode == 4 { 8 } else { 7 };
    (
        StatusCode::CREATED,
        Json(
            json!({"token":"fake-installation-token", "expires_at":expiry.format(&time::format_description::well_known::Rfc3339).unwrap(), "permissions":permissions, "repositories":[{"id":repo}]}),
        ),
    )
}
fn keys() -> (tempfile::TempDir, Vec<u8>, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let private = dir.path().join("test-key.pem");
    let public = dir.path().join("test-public.pem");
    let output = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
        ])
        .arg(&private)
        .output()
        .unwrap();
    assert!(output.status.success());
    let output = Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&private)
        .arg("-out")
        .arg(&public)
        .output()
        .unwrap();
    assert!(output.status.success());
    let a = std::fs::read(private).unwrap();
    let b = std::fs::read(public).unwrap();
    (dir, a, b)
}
#[tokio::test]
async fn creates_signed_scoped_tokens_and_rejects_unsafe_responses() {
    let (key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let pulls = Arc::new(AtomicUsize::new(0));
    let permissions = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/branches/main", get(reconcile_branch))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route("/repos/owner/repo/pulls", get(open_pulls))
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/2",
            get(actions_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/2/jobs",
            get(actions_jobs),
        )
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(permission),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: pulls.clone(),
            permissions: permissions.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = client.read_token(42, 7).await.unwrap();
    assert_eq!(token.secret(), "fake-installation-token");
    assert!(!format!("{token:?}").contains(token.secret()));
    let actions = client
        .observe_actions_attempt(&token, "owner", "repo", 123, 2)
        .await
        .unwrap();
    assert_eq!(actions.run.head_sha, "a".repeat(40));
    assert_eq!(actions.jobs.len(), 1);
    mode.store(15, Ordering::SeqCst);
    assert_eq!(
        client
            .observe_actions_attempt(&token, "owner", "repo", 123, 2)
            .await
            .unwrap()
            .jobs
            .len(),
        101
    );
    for failure in [11, 12, 13, 14] {
        mode.store(failure, Ordering::SeqCst);
        pulls.store(0, Ordering::SeqCst);
        assert!(
            matches!(
                client
                    .observe_actions_attempt(&token, "owner", "repo", 123, 2)
                    .await,
                Err(GitHubError::UnstableObservation)
            ),
            "mode {failure}"
        );
    }
    mode.store(0, Ordering::SeqCst);
    let snapshot = client
        .observe_pull(&token, "owner", "repo", 19)
        .await
        .unwrap();
    assert_eq!(snapshot.pull.head.sha, "a".repeat(40));
    assert_eq!(snapshot.reviews.len(), 1);
    mode.store(6, Ordering::SeqCst);
    assert_eq!(
        client
            .observe_pull(&token, "owner", "repo", 19)
            .await
            .unwrap()
            .reviews
            .len(),
        101
    );
    mode.store(5, Ordering::SeqCst);
    pulls.store(0, Ordering::SeqCst);
    assert!(matches!(
        client.observe_pull(&token, "owner", "repo", 19).await,
        Err(GitHubError::UnstableObservation)
    ));
    assert!(matches!(
        client.observe_pull(&token, "../owner", "repo", 19).await,
        Err(GitHubError::InvalidConfiguration)
    ));
    mode.store(0, Ordering::SeqCst);
    let authorized = client
        .observe_authorized_pull(&token, "owner", "repo", 19)
        .await
        .unwrap();
    assert_eq!(
        authorized.authorized_reviewers,
        std::collections::BTreeSet::from([9])
    );
    let policy = sluss::admission::Policy {
        target_branch: "main".into(),
        required_reviews: 1,
        block_labels: Default::default(),
        authorized_reviewers: authorized.authorized_reviewers,
    };
    assert!(sluss::admission::evaluate(&authorized.snapshot, &policy).is_ok());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&path).unwrap();
    let mut store = sluss::store::Store::open(&path).unwrap();
    inbox
        .record("reconcile-1", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    let config = sluss::reconcile::RepositoryPolicy {
        installation: 42,
        repository: 7,
        owner: "owner".into(),
        name: "repo".into(),
        target_branch: "main".into(),
        required_reviews: 1,
        block_labels: std::collections::BTreeSet::from(["do-not-merge".into()]),
    };
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
        .await
        .unwrap();
    assert_eq!(store.candidates().unwrap()[0].head_sha, "a".repeat(40));
    assert!(inbox.pending().unwrap().is_empty());
    assert!(matches!(
        store.plan_batch("batch-4123456789abcdef", &"c".repeat(40), 4, 1),
        Err(sluss::store::StoreError::StaleBatch)
    ));
    let obsolete = store
        .plan_batch("batch-3123456789abcdef", &"b".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    inbox
        .record("target-moved", "push", &[11; 32], 42, 7)
        .unwrap();
    let moving_job = inbox.pending().unwrap().remove(0);
    pulls.store(0, Ordering::SeqCst);
    mode.store(35, Ordering::SeqCst);
    assert!(
        sluss::reconcile::reconcile_once(&client, &token, &mut store, &moving_job, &config)
            .await
            .is_err()
    );
    assert_eq!(inbox.pending().unwrap().len(), 1);
    mode.store(34, Ordering::SeqCst);
    assert!(
        sluss::reconcile::reconcile_once(&client, &token, &mut store, &moving_job, &config)
            .await
            .is_err()
    );
    assert_eq!(
        store.batch(&obsolete.id).unwrap().unwrap().0.state,
        sluss::batch::State::Planned
    );
    mode.store(36, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &moving_job, &config)
        .await
        .unwrap();
    assert!(matches!(
        store.batch(&obsolete.id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    assert!(matches!(
        store.plan_batch("batch-4123456789abcdef", &"b".repeat(40), 4, 1),
        Err(sluss::store::StoreError::StaleBatch)
    ));
    inbox
        .record("target-restored", "push", &[12; 32], 42, 7)
        .unwrap();
    let restored = inbox.pending().unwrap().remove(0);
    mode.store(0, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &restored, &config)
        .await
        .unwrap();
    let frozen = store
        .plan_batch("batch-0123456789abcdef", &"b".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    inbox
        .record("reconcile-other", "pull_request", &[3; 32], 42, 7)
        .unwrap();
    let other_job = inbox.pending().unwrap().remove(0);
    let other_app = App::new("different-app".into(), &private, &format!("http://{addr}/")).unwrap();
    let other_token = other_app.read_token(42, 7).await.unwrap();
    assert!(matches!(
        sluss::reconcile::reconcile_once(&other_app, &other_token, &mut store, &other_job, &config)
            .await,
        Err(sluss::reconcile::ReconcileError::Store(
            sluss::store::StoreError::PolicyMismatch
        ))
    ));
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &other_job, &config)
        .await
        .unwrap();
    assert_eq!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Planned
    );
    inbox
        .record("permission-changed", "repository", &[6; 32], 42, 7)
        .unwrap();
    let permission_job = inbox.pending().unwrap().remove(0);
    mode.store(25, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &permission_job, &config)
        .await
        .unwrap();
    assert!(matches!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    let frozen = store
        .plan_batch("batch-1123456789abcdef", &"b".repeat(40), 4, 1)
        .unwrap()
        .unwrap();
    mode.store(0, Ordering::SeqCst);
    inbox
        .record("review-comment", "pull_request_review", &[7; 32], 42, 7)
        .unwrap();
    let review_job = inbox.pending().unwrap().remove(0);
    mode.store(26, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &review_job, &config)
        .await
        .unwrap();
    assert_eq!(store.candidates().unwrap()[0].head_sha, "a".repeat(40));
    assert!(matches!(
        store.batch(&frozen.id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    mode.store(0, Ordering::SeqCst);
    inbox
        .record("reconcile-2", "pull_request", &[2; 32], 42, 7)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    mode.store(8, Ordering::SeqCst);
    assert!(
        sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
            .await
            .is_err()
    );
    assert_eq!(store.candidates().unwrap().len(), 1);
    assert_eq!(inbox.pending().unwrap().len(), 1);
    mode.store(10, Ordering::SeqCst);
    sluss::reconcile::reconcile_once(&client, &token, &mut store, &job, &config)
        .await
        .unwrap();
    assert!(store.candidates().unwrap().is_empty());
    mode.store(7, Ordering::SeqCst);
    assert!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await
            .unwrap()
            .authorized_reviewers
            .is_empty()
    );
    mode.store(8, Ordering::SeqCst);
    assert!(matches!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await,
        Err(GitHubError::UnstableObservation)
    ));
    permissions.store(0, Ordering::SeqCst);
    mode.store(9, Ordering::SeqCst);
    assert!(matches!(
        client
            .observe_authorized_pull(&token, "owner", "repo", 19)
            .await,
        Err(GitHubError::UnstableObservation)
    ));
    mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        client.read_token(42, 7).await,
        Err(GitHubError::Status(307))
    ));
    for value in [2, 3, 4, 226, 227] {
        mode.store(value, Ordering::SeqCst);
        assert!(matches!(
            client.read_token(42, 7).await,
            Err(GitHubError::InvalidResponse)
        ));
    }
    mode.store(226, Ordering::SeqCst);
    assert!(matches!(
        client.dispatch_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    assert!(matches!(
        client.staging_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    assert!(matches!(
        client.runner_inventory_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    assert!(matches!(
        client.checks_write_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    assert!(matches!(
        client.read_token(0, 7).await,
        Err(GitHubError::InvalidConfiguration)
    ));
    mode.store(0, Ordering::SeqCst);
    let cli_dir = tempfile::tempdir().unwrap();
    let cli_db = cli_dir.path().join("cli.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&cli_db).unwrap();
    inbox.record("cli", "push", &[9; 32], 42, 7).unwrap();
    let path = cli_db.clone();
    let key_path = key_dir.path().join("test-key.pem");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(path)
            .args([
                "reconcile",
                "--owner",
                "owner",
                "--repository",
                "repo",
                "--app-client-id",
                "app-client-id",
                "--installation-id",
                "42",
                "--repository-id",
                "7",
                "--api",
                &format!("http://{addr}/"),
            ])
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_path)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        sluss::store::Store::open(&cli_db)
            .unwrap()
            .candidates()
            .unwrap()
            .len(),
        1
    );
    assert!(inbox.pending().unwrap().is_empty());
    server.abort();
}
#[test]
fn rejects_insecure_and_malformed_configuration() {
    let (_dir, private, _public) = keys();
    for url in [
        "http://example.com/",
        "http://localhost/",
        "https://user:pass@example.com/",
        "https://example.com/path",
        "https://example.com/?q=1",
    ] {
        assert!(matches!(
            App::new("id".into(), &private, url),
            Err(GitHubError::InvalidConfiguration)
        ));
    }
    assert!(matches!(
        App::new("id".into(), b"bad key", "https://api.github.com/"),
        Err(GitHubError::InvalidConfiguration)
    ));
}

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn_service(
    db: &std::path::Path,
    config: &std::path::Path,
    key_dir: &std::path::Path,
    port: u16,
) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(db)
        .args([
            "serve",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--config",
        ])
        .arg(config)
        .arg("--auto-plan")
        .env("SLUSS_WEBHOOK_SECRET", "test-secret")
        .env(
            "SLUSS_GITHUB_PRIVATE_KEY_PATH",
            key_dir.join("test-key.pem"),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    ChildGuard(child)
}
#[tokio::test]
async fn configured_service_recovers_by_scanning_and_processes_webhooks() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let (key_dir, _private, public) = keys();
    let mode = Arc::new(AtomicU8::new(1));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let api = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/branches/main", get(reconcile_branch))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route("/repos/owner/repo/pulls", get(open_pulls))
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(permission),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: token_calls.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_addr = listener.local_addr().unwrap();
    let api_task = tokio::spawn(async move { axum::serve(listener, api).await.unwrap() });
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config,format!("version = 1\n[github]\napp_client_id = \"app-client-id\"\ninstallation_id = 42\nrepository_id = 7\nowner = \"owner\"\nrepository = \"repo\"\napi = \"http://{api_addr}/\"\n[queue]\ntarget = \"main\"\ndebounce_seconds = 0\nbatch_window_seconds = 0\npoll_seconds = 1\nrescan_seconds = 30\n[checks]\nrequired_reviews = 1\nblock_labels = [\"do-not-merge\"]\n")).unwrap();
    let mut child = spawn_service(&db, &config, key_dir.path(), port);
    let client = reqwest::Client::new();
    let mut failed_pending = false;
    for _ in 0..150 {
        if token_calls.load(Ordering::SeqCst) > 0
            && sluss::webhook::Inbox::open(&db)
                .ok()
                .and_then(|i| i.pending_for(42, 7).ok().flatten())
                .is_some()
        {
            failed_pending = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before failed reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed_pending, "failed work was not retained");
    drop(child);
    mode.store(0, Ordering::SeqCst);
    let mut child = spawn_service(&db, &config, key_dir.path(), port);
    let mut ready = false;
    for _ in 0..150 {
        if let Ok(store) = sluss::store::Store::open(&db)
            && store.candidates().is_ok_and(|v| v.len() == 1)
        {
            ready = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before startup reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(ready, "startup rescan did not populate the queue");
    let mut batch_id = None;
    for _ in 0..150 {
        if let Ok(store) = sluss::store::Store::open(&db)
            && let Ok(batches) = store.active_batches()
            && batches.len() == 1
        {
            assert_eq!(batches[0].candidates[0].number, 19);
            batch_id = Some(batches[0].id.clone());
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before automatic planning");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let batch_id = batch_id.expect("automatic planner did not create a batch");
    drop(child);
    let mut child = spawn_service(&db, &config, key_dir.path(), port);
    for _ in 0..50 {
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited after planner restart");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let batches = sluss::store::Store::open(&db)
        .unwrap()
        .active_batches()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].id, batch_id);
    mode.store(10, Ordering::SeqCst);
    let body = br#"{"installation":{"id":42},"repository":{"id":7}}"#;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"test-secret").unwrap();
    mac.update(body);
    let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let response = client
        .post(format!("http://127.0.0.1:{port}/webhook"))
        .header("X-Hub-Signature-256", signature)
        .header("X-GitHub-Delivery", "service-test")
        .header("X-GitHub-Event", "pull_request")
        .body(body.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let mut cleared = false;
    for _ in 0..150 {
        if let Ok(store) = sluss::store::Store::open(&db)
            && store.candidates().is_ok_and(|v| v.is_empty())
        {
            cleared = true;
            break;
        }
        if child.0.try_wait().unwrap().is_some() {
            panic!("service exited before webhook reconciliation");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(cleared, "webhook reconciliation did not remove blocked PR");
    assert!(
        sluss::store::Store::open(&db)
            .unwrap()
            .active_batches()
            .unwrap()
            .is_empty()
    );
    api_task.abort();
}

#[tokio::test]
async fn github_staging_marks_changed_policy_as_a_deterministic_batch_failure() {
    use sluss::{
        batch::{Candidate, State},
        coordinator::{self, GitHubRepository, PrepareError},
        integration::PolicyError,
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (_key_dir, private, public) = keys();
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: Arc::new(AtomicU8::new(0)),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = client.staging_token(42, 7).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("source");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-b", "main"]);
    std::fs::write(repo.join("policy"), "trusted\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "base"]);
    let base = git(&["rev-parse", "HEAD"]);
    git(&["checkout", "-b", "change"]);
    std::fs::write(repo.join("policy"), "changed\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "policy change"]);
    let head = git(&["rev-parse", "HEAD"]);
    let db = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: Candidate {
                    number: 1,
                    head_sha: head,
                },
                evidence_digest: "e".repeat(64),
            }],
        )
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    assert!(matches!(
        coordinator::prepare_github(
            &mut store,
            &client,
            &token,
            GitHubRepository {
                owner: "owner",
                repository: "repo"
            },
            id,
            &repo,
            &["policy".into()]
        )
        .await,
        Err(PrepareError::Policy(PolicyError::Changed))
    ));
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        State::Failed { .. }
    ));
    assert_eq!(store.failed_candidates().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn uncertain_dispatch_discovery_is_read_only_scoped_and_never_asserts_origin() {
    use sluss::{
        batch::Candidate,
        scheduler::{Capacity, DispatchPlan, DispatchState, Host, Request, Scheduler},
        store::{AdmissionCandidate, Store},
        webhook::Inbox,
    };
    let (key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route(
            "/repos/owner/repo/actions/workflows/17/runs",
            get(discovery_runs),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads.clone(),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let read_token = client.read_token(42, 7).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("event", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &[AdmissionCandidate {
                candidate: Candidate {
                    number: 1,
                    head_sha: "c".repeat(40),
                },
                evidence_digest: "d".repeat(64),
            }],
            &"b".repeat(40),
            "main",
        )
        .unwrap();
    let id = "batch-0123456789abcdef";
    store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let mut scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: [7].into(),
                    pools: pools.clone(),
                },
            )]),
        },
    )
    .unwrap();
    let plan = DispatchPlan {
        installation_id: 42,
        repository_id: 7,
        workflow_id: 17,
        integration_sha: "a".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "e".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    scheduler
        .reserve_batch_dispatch(id, &Request { pools }, &plan)
        .unwrap()
        .unwrap();
    scheduler
        .claim_batch_dispatch(id, ".github/workflows/ci.yml")
        .unwrap();
    let frozen = Scheduler::inspect_uncertain_dispatch(&db, id).unwrap();
    let candidates = client
        .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &frozen)
        .await
        .unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|r| (r.id, r.run_attempt))
            .collect::<Vec<_>>(),
        vec![(123, 1), (125, 2)]
    );
    assert_eq!(
        Scheduler::inspect_uncertain_dispatch(&db, id).unwrap(),
        frozen
    );
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    assert_eq!(scheduler.receipt_source(id).unwrap(), None);
    for bad in [220, 221, 222] {
        mode.store(bad, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        assert!(
            client
                .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &frozen)
                .await
                .is_err(),
            "mode {bad}"
        );
    }
    mode.store(223, Ordering::SeqCst);
    assert!(
        client
            .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &frozen)
            .await
            .unwrap()
            .is_empty()
    );
    mode.store(224, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &frozen)
            .await
            .unwrap()[0]
            .status,
        "completed"
    );
    mode.store(225, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &frozen)
            .await
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        vec![101]
    );
    mode.store(0, Ordering::SeqCst);
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    let db_arg = db.clone();
    let key = key_dir.path().join("test-key.pem");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args(["find-dispatch-runs", "--batch-id", id, "--config"])
            .arg(config)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("run 123: latest attempt 1"));
    assert!(text.contains("absence does not prove"));
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    // Earlier journals may not have the workflow-path table. Discovery must
    // remain read-only and explicitly retain the weaker, unbound result.
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute("DROP TABLE dispatch_workflows", []).unwrap();
    let legacy = Scheduler::inspect_uncertain_dispatch(&db, id).unwrap();
    assert_eq!(legacy.workflow_path, None);
    let legacy_runs = client
        .find_uncertain_dispatch_runs(&read_token, "owner", "repo", &legacy)
        .await
        .unwrap();
    assert_eq!(
        legacy_runs.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![123, 125, 127]
    );
    let table_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='dispatch_workflows')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!table_exists);
    assert!(Scheduler::inspect_uncertain_dispatch(dir.path().join("missing.sqlite"), id).is_err());
    assert!(!dir.path().join("missing.sqlite").exists());
    server.abort();
}

#[tokio::test]
async fn dispatch_is_scoped_claimed_once_and_uncertain_responses_are_not_retried() {
    use sluss::github::DispatchError;
    use sluss::scheduler::{Capacity, DispatchPlan, DispatchState, Host, Request, Scheduler};
    let (_key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let posts = Arc::new(AtomicUsize::new(0));
    let run_reads = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(dispatch_ref),
        )
        .route(
            "/repos/owner/repo/actions/workflows/17/dispatches",
            post(dispatch_post),
        )
        .route("/repos/owner/repo/actions/runs/123", get(dispatched_run))
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1/jobs",
            get(actions_jobs),
        )
        .route(
            "/repos/owner/repo/actions/runs/124/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/124/attempts/1/jobs",
            get(actions_jobs),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: run_reads.clone(),
            permissions: posts.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let read_token = client.read_token(42, 7).await.unwrap();
    let write_token = client.dispatch_token(42, 7).await.unwrap();
    let db = tempfile::tempdir().unwrap();
    let pools = BTreeMap::from([("rust".into(), 3)]);
    let mut scheduler = Scheduler::open(
        db.path().join("db"),
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 3,
                    runner_ids: Default::default(),
                    pools,
                },
            )]),
        },
    )
    .unwrap();
    let plan = DispatchPlan {
        installation_id: 42,
        repository_id: 7,
        workflow_id: 17,
        integration_sha: "a".repeat(40),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        policy_digest: "b".repeat(64),
        required_jobs: std::collections::BTreeSet::from(["job-1-0".into()]),
        suites: std::collections::BTreeSet::from(["full".into()]),
        selection_policy_digest: None,
    };
    let req = Request {
        pools: BTreeMap::from([("rust".into(), 1)]),
    };
    scheduler
        .reserve_dispatch("good", &req, &plan)
        .unwrap()
        .unwrap();
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::Invalid)
    ));
    let other = App::new("different-app".into(), &private, &format!("http://{addr}/")).unwrap();
    assert!(matches!(
        other
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::GitHub(GitHubError::InvalidConfiguration))
    ));
    assert_eq!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await
            .unwrap(),
        123
    );
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Accepted { run_id: 123 }
    );
    assert!(matches!(
        scheduler.observe_dispatch("good", 124),
        Err(sluss::scheduler::SchedulerError::DispatchTransition)
    ));
    for bad_mode in [18, 19, 20, 21, 22, 23] {
        mode.store(bad_mode, Ordering::SeqCst);
        run_reads.store(0, Ordering::SeqCst);
        assert!(
            client
                .reconcile_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
                .await
                .is_err(),
            "mode {bad_mode}"
        );
        assert_eq!(
            scheduler.pending_dispatches().unwrap()[0].state,
            DispatchState::Accepted { run_id: 123 }
        );
    }
    mode.store(0, Ordering::SeqCst);
    let snapshot = client
        .reconcile_dispatch(&mut scheduler, &read_token, "owner", "repo", "good")
        .await
        .unwrap();
    assert_eq!(snapshot.run.head_sha, plan.integration_sha);
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Observed { run_id: 123 }
    );
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "good")
            .await,
        Err(DispatchError::AlreadyClaimed)
    ));
    scheduler
        .reserve_dispatch("lost", &req, &plan)
        .unwrap()
        .unwrap();
    mode.store(17, Ordering::SeqCst);
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "lost")
            .await,
        Err(DispatchError::Uncertain)
    ));
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "lost")
            .await,
        Err(DispatchError::AlreadyClaimed)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    scheduler
        .reserve_dispatch("stale", &req, &plan)
        .unwrap()
        .unwrap();
    mode.store(16, Ordering::SeqCst);
    assert!(matches!(
        client
            .issue_dispatch(&mut scheduler, &write_token, "owner", "repo", "stale")
            .await,
        Err(DispatchError::StaleRef)
    ));
    let pending = scheduler.pending_dispatches().unwrap();
    assert_eq!(
        pending
            .iter()
            .find(|p| p.reservation.id == "lost")
            .unwrap()
            .state,
        DispatchState::Sending
    );
    assert_eq!(
        pending
            .iter()
            .find(|p| p.reservation.id == "stale")
            .unwrap()
            .state,
        DispatchState::Prepared
    );
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    let no_content_db = tempfile::tempdir().unwrap();
    let mut no_content_scheduler = Scheduler::open(
        no_content_db.path().join("db"),
        Capacity {
            pools: BTreeMap::from([("rust".into(), 1)]),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: Default::default(),
                    pools: BTreeMap::from([("rust".into(), 1)]),
                },
            )]),
        },
    )
    .unwrap();
    no_content_scheduler
        .reserve_dispatch("no-content", &req, &plan)
        .unwrap()
        .unwrap();
    mode.store(39, Ordering::SeqCst);
    assert!(matches!(
        client
            .issue_dispatch(
                &mut no_content_scheduler,
                &write_token,
                "owner",
                "repo",
                "no-content"
            )
            .await,
        Err(DispatchError::Uncertain)
    ));
    assert_eq!(
        no_content_scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    assert!(matches!(
        client
            .issue_dispatch(
                &mut no_content_scheduler,
                &write_token,
                "owner",
                "repo",
                "no-content"
            )
            .await,
        Err(DispatchError::AlreadyClaimed)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 3);
    mode.store(23, Ordering::SeqCst);
    assert!(matches!(
        client
            .recover_uncertain_dispatch(
                &mut no_content_scheduler,
                &read_token,
                "owner",
                "repo",
                "no-content",
                123
            )
            .await,
        Err(DispatchError::RunMismatch)
    ));
    assert_eq!(
        no_content_scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    assert_eq!(
        no_content_scheduler.receipt_source("no-content").unwrap(),
        None
    );
    mode.store(0, Ordering::SeqCst);
    client
        .recover_uncertain_dispatch(
            &mut no_content_scheduler,
            &read_token,
            "owner",
            "repo",
            "no-content",
            123,
        )
        .await
        .unwrap();
    assert_eq!(
        no_content_scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Observed { run_id: 123 }
    );
    assert_eq!(
        no_content_scheduler
            .receipt_source("no-content")
            .unwrap()
            .as_deref(),
        Some("recovered")
    );
    assert_eq!(posts.load(Ordering::SeqCst), 3);
    assert_eq!(
        scheduler.receipt_source("good").unwrap().as_deref(),
        Some("direct")
    );
    server.abort();
}

#[tokio::test]
async fn staging_tokens_are_scoped_and_never_sent_from_a_non_github_origin() {
    use sluss::github::StagingError;
    let (_key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: Arc::new(AtomicUsize::new(0)),
            permissions: Arc::new(AtomicUsize::new(0)),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let stage_token = client.staging_token(42, 7).await.unwrap();
    assert!(!format!("{stage_token:?}").contains(stage_token.secret()));
    mode.store(27, Ordering::SeqCst);
    assert!(matches!(
        client.staging_token(42, 7).await,
        Err(GitHubError::InvalidResponse)
    ));
    mode.store(0, Ordering::SeqCst);
    let source = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(source.path())
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    };
    git(&["init", "-b", "main"]);
    std::fs::write(source.path().join("base"), "base").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "base"]);
    let base = git(&["rev-parse", "HEAD"]);
    std::fs::write(source.path().join("next"), "next").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "next"]);
    let head = git(&["rev-parse", "HEAD"]);
    let integration =
        sluss::integration::build(source.path(), &base, std::slice::from_ref(&head)).unwrap();
    assert!(matches!(
        client
            .stage_integration(
                &stage_token,
                "owner",
                "repo",
                integration,
                "batch-0123456789abcdef"
            )
            .await,
        Err(StagingError::InvalidScope)
    ));
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let mut inbox = sluss::webhook::Inbox::open(&db).unwrap();
    let mut store = sluss::store::Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let admitted = sluss::store::AdmissionCandidate {
        candidate: sluss::batch::Candidate {
            number: 1,
            head_sha: head,
        },
        evidence_digest: "f".repeat(64),
    };
    store
        .apply_admission_reconciliation(&event, &"e".repeat(64), &[admitted])
        .unwrap();
    let id = "batch-0123456789abcdef";
    store.plan_batch(id, &base, 1, 1).unwrap().unwrap();
    assert!(matches!(
        sluss::coordinator::prepare_github(
            &mut store,
            &client,
            &stage_token,
            sluss::coordinator::GitHubRepository {
                owner: "owner",
                repository: "repo"
            },
            id,
            source.path(),
            &["base".into()]
        )
        .await,
        Err(sluss::coordinator::PrepareError::GitHubStage(
            StagingError::InvalidScope
        ))
    ));
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        sluss::batch::State::Building
    );
    server.abort();
}

#[test]
fn cli_dispatches_and_observes_a_reserved_host_against_stub_github() {
    // This end-to-end fixture holds many independent async flows and exceeds
    // the default test-thread stack; keep the real workflow on a bounded stack.
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(Box::pin(cli_dispatches_fixture()));
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn cli_dispatches_fixture() {
    use sluss::{
        selective::{FrozenSelection, Policy, Selection},
        store::Store,
        webhook::Inbox,
    };
    let (key_dir, private, public) = keys();
    let posts = Arc::new(AtomicUsize::new(0));
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let graphql_counts = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/app", get(authenticated_app))
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/graphql", post(graphql_schema))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/check-runs/333", get(verified_check))
        .route(
            "/repos/owner/repo/check-runs/{check_id}",
            get(failed_check_get).patch(cancel_pending_check_patch),
        )
        .route("/repos/owner/repo/check-runs", post(failed_check_post))
        .route("/repos/owner/repo/actions/runners", get(repository_runners))
        .route("/repos/owner/repo/branches/main", get(doctor_branch))
        .route("/repos/owner/repo/pulls", get(open_pulls))
        .route("/repos/owner/repo/pulls/19", get(pull))
        .route("/repos/owner/repo/pulls/19/reviews", get(reviews))
        .route("/repos/owner/repo/pulls/20", get(pull20))
        .route("/repos/owner/repo/pulls/20/reviews", get(reviews))
        .route(
            "/repos/owner/repo/collaborators/reviewer/permission",
            get(dispatch_permission),
        )
        .route(
            "/repos/owner/repo/actions/workflows/17",
            get(doctor_workflow),
        )
        .route("/repos/owner/repo/rules/branches/main", get(doctor_rules))
        .route(
            "/repos/owner/repo/branches/main/protection",
            get(doctor_protection),
        )
        .route("/repos/owner/repo/rulesets/10", get(doctor_ruleset))
        .route(
            "/repos/owner/repo/git/ref/heads/feature/19",
            get(guarded_head_ref),
        )
        .route(
            "/repos/owner/repo/git/ref/pull/19/head",
            get(guarded_head_ref),
        )
        .route(
            "/repos/owner/repo/git/refs",
            post(create_landing_marker_stub),
        )
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/receipts/{marker_id}",
            get(landing_marker_preflight),
        )
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(dispatch_ref),
        )
        .route(
            "/repos/owner/repo/actions/workflows/17/dispatches",
            post(dispatch_post),
        )
        .route("/repos/owner/repo/actions/runs/123", get(dispatched_run))
        .route("/repos/owner/repo/actions/runs/124", get(dispatched_run))
        .route(
            "/repos/owner/repo/actions/runs/123/cancel",
            post(cancel_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/124/cancel",
            post(cancel_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/123/attempts/1/jobs",
            get(actions_jobs),
        )
        .route(
            "/repos/owner/repo/actions/runs/124/attempts/1",
            get(dispatched_run),
        )
        .route(
            "/repos/owner/repo/actions/runs/124/attempts/1/jobs",
            get(actions_jobs),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads.clone(),
            permissions: posts.clone(),
            token_calls: graphql_counts.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let data = tempfile::tempdir().unwrap();
    let db = data.path().join("queue.sqlite");
    let config = data.path().join("sluss.toml");
    let capacity = data.path().join("capacity.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    std::fs::write(&capacity, "[pools]\nrust=1\n[hosts.sluss-host-test]\nslots=1\nrunner_ids=[1]\n[hosts.sluss-host-test.pools]\nrust=1\n").unwrap();
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    inbox
        .record("initial", "pull_request", &[1; 32], 42, 7)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let fixture_app =
        App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    assert_eq!(fixture_app.observe_app_identity().await.unwrap().id, 17);
    for invalid in [80, 81, 82] {
        mode.store(invalid, Ordering::SeqCst);
        assert!(fixture_app.observe_app_identity().await.is_err());
    }
    mode.store(0, Ordering::SeqCst);
    let fixture_token = fixture_app.read_token(42, 7).await.unwrap();
    let security_token = fixture_app.runner_inventory_token(42, 7).await.unwrap();
    let expected_check = sluss::github::AppCheckExpectation {
        installation_id: 42,
        repository_id: 7,
        repository_node_id: "R_repo".into(),
        check_run_id: 333,
        integration_sha: "a".repeat(40),
        batch_id: "batch-0123456789abcdef".into(),
    };
    assert_eq!(
        fixture_app
            .observe_own_verified_check(&fixture_token, "owner", "repo", &expected_check)
            .await
            .unwrap()
            .app
            .id,
        17
    );
    assert!(matches!(
        fixture_app
            .observe_own_verified_check(&security_token, "owner", "repo", &expected_check)
            .await,
        Err(GitHubError::InvalidConfiguration)
    ));
    let mut invalid_expectations = Vec::new();
    let mut item = expected_check.clone();
    item.installation_id = 43;
    invalid_expectations.push(item);
    let mut item = expected_check.clone();
    item.repository_id = 8;
    invalid_expectations.push(item);
    let mut item = expected_check.clone();
    item.check_run_id = 0;
    invalid_expectations.push(item);
    let mut item = expected_check.clone();
    item.integration_sha = "0".repeat(40);
    invalid_expectations.push(item);
    let mut item = expected_check.clone();
    item.batch_id = "../batch".into();
    invalid_expectations.push(item);
    for item in invalid_expectations {
        assert!(matches!(
            fixture_app
                .observe_own_verified_check(&fixture_token, "owner", "repo", &item)
                .await,
            Err(GitHubError::InvalidConfiguration)
        ));
    }
    for invalid in [63, 80, 81, 82, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93] {
        mode.store(invalid, Ordering::SeqCst);
        assert!(
            fixture_app
                .observe_own_verified_check(&fixture_token, "owner", "repo", &expected_check)
                .await
                .is_err(),
            "mode {invalid}"
        );
    }
    mode.store(0, Ordering::SeqCst);
    assert!(
        fixture_app
            .observe_branch_security(&fixture_token, "owner", "repo", "main")
            .await
            .is_err()
    );
    let security = fixture_app
        .observe_branch_security(&security_token, "owner", "repo", "main")
        .await
        .unwrap();
    assert_eq!(security.rulesets.len(), 1);
    assert!(security.rulesets[0].bypass_visible);
    assert!(matches!(
        security.protection,
        sluss::github::ProtectionObservation::Present(_)
    ));
    mode.store(67, Ordering::SeqCst);
    assert!(
        !fixture_app
            .observe_branch_security(&security_token, "owner", "repo", "main")
            .await
            .unwrap()
            .rulesets[0]
            .bypass_visible
    );
    mode.store(73, Ordering::SeqCst);
    let empty = fixture_app
        .observe_branch_security(&security_token, "owner", "repo", "main")
        .await
        .unwrap();
    assert!(empty.rulesets[0].bypass_visible);
    assert_eq!(empty.rulesets[0].detail["bypass_actors"], json!([]));
    mode.store(69, Ordering::SeqCst);
    assert!(matches!(
        fixture_app
            .observe_branch_security(&security_token, "owner", "repo", "main")
            .await
            .unwrap()
            .protection,
        sluss::github::ProtectionObservation::Unavailable(404)
    ));
    for invalid in [68, 71, 72, 74] {
        mode.store(invalid, Ordering::SeqCst);
        assert!(
            fixture_app
                .observe_branch_security(&security_token, "owner", "repo", "main")
                .await
                .is_err()
        );
    }
    mode.store(0, Ordering::SeqCst);
    assert!(
        fixture_app
            .inspect_ref_update_schema(&fixture_token)
            .await
            .unwrap()
    );
    mode.store(60, Ordering::SeqCst);
    assert!(
        !fixture_app
            .inspect_ref_update_schema(&fixture_token)
            .await
            .unwrap()
    );
    for invalid in [61, 62] {
        mode.store(invalid, Ordering::SeqCst);
        assert!(matches!(
            fixture_app.inspect_ref_update_schema(&fixture_token).await,
            Err(GitHubError::UnstableObservation)
        ));
    }
    mode.store(0, Ordering::SeqCst);
    let fixture_policy = sluss::config::Config::load(&config).unwrap().policy();
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut store,
        &event,
        &fixture_policy,
    )
    .await
    .unwrap();
    let id = "batch-0123456789abcdef";
    // Exercise the actual dispatch path with two independent hosts: a busy
    // registered runner must not prevent dispatch to the other idle runner.
    let independent_db = data.path().join("independent.sqlite");
    let mut independent_inbox = Inbox::open(&independent_db).unwrap();
    let mut independent_store = Store::open(&independent_db).unwrap();
    independent_inbox
        .record("independent", "pull_request", &[7; 32], 42, 7)
        .unwrap();
    let event = independent_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut independent_store,
        &event,
        &fixture_policy,
    )
    .await
    .unwrap();
    independent_store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    independent_store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    independent_store.start_integration(id).unwrap();
    independent_store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    let independent_capacity: sluss::scheduler::Capacity = toml::from_str(
        "[pools]\nrust=2\n[hosts.sluss-host-busy]\nslots=1\nrunner_ids=[2]\n[hosts.sluss-host-busy.pools]\nrust=1\n[hosts.sluss-host-test]\nslots=1\nrunner_ids=[1]\n[hosts.sluss-host-test.pools]\nrust=1\n"
    ).unwrap();
    let settings = sluss::dispatch::Settings {
        workflow_id: 17,
        required_jobs: ["job-1-0".into()].into(),
        request: sluss::scheduler::Request {
            pools: [("rust".into(), 1)].into(),
        },
        verification_policy: None,
    };
    mode.store(45, Ordering::SeqCst);
    assert_eq!(
        sluss::dispatch::batch(
            &independent_db,
            &fixture_app,
            &fixture_policy,
            &independent_capacity,
            &settings,
            id
        )
        .await
        .unwrap(),
        sluss::dispatch::Outcome::NoCapacity
    );
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    mode.store(44, Ordering::SeqCst);
    let snapshot_conn = rusqlite::Connection::open(&independent_db).unwrap();
    let frozen_paths: String = snapshot_conn
        .query_row(
            "SELECT paths FROM batch_policy_paths WHERE batch_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    snapshot_conn
        .execute("DELETE FROM batch_policy_paths WHERE batch_id=?1", [id])
        .unwrap();
    let uncovered = sluss::dispatch::batch(
        &independent_db,
        &fixture_app,
        &fixture_policy,
        &independent_capacity,
        &settings,
        id,
    )
    .await;
    assert!(matches!(
        uncovered,
        Err(sluss::dispatch::Error::Dispatch(
            sluss::github::DispatchError::Scheduler(
                sluss::scheduler::SchedulerError::WorkflowPolicyMismatch
            )
        ))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    snapshot_conn
        .execute(
            "INSERT INTO batch_policy_paths(batch_id,paths) VALUES(?1,?2)",
            rusqlite::params![id, frozen_paths],
        )
        .unwrap();
    for rejected_mode in [48, 49, 50] {
        reads.store(0, Ordering::SeqCst);
        mode.store(rejected_mode, Ordering::SeqCst);
        let rejected = sluss::dispatch::batch(
            &independent_db,
            &fixture_app,
            &fixture_policy,
            &independent_capacity,
            &settings,
            id,
        )
        .await;
        assert!(rejected.is_err(), "mode {rejected_mode}");
        assert_eq!(
            posts.load(Ordering::SeqCst),
            0,
            "untrusted registration must not be dispatched"
        );
        let scheduler =
            sluss::scheduler::Scheduler::open(&independent_db, independent_capacity.clone())
                .unwrap();
        assert!(
            scheduler
                .pending_dispatches()
                .unwrap()
                .iter()
                .any(|item| item.reservation.id == id
                    && item.state == sluss::scheduler::DispatchState::Prepared)
        );
    }
    mode.store(66, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    assert_eq!(
        sluss::dispatch::batch(
            &independent_db,
            &fixture_app,
            &fixture_policy,
            &independent_capacity,
            &settings,
            id
        )
        .await
        .unwrap(),
        sluss::dispatch::Outcome::Sent {
            run: 123,
            host: "sluss-host-test".into()
        }
    );
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let has_rules: bool = snapshot_conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM dispatch_branch_rules WHERE id=?1)",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        !has_rules,
        "unstable rules allow CI-only dispatch but must not become landing evidence"
    );
    let metrics = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&independent_db)
        .arg("metrics")
        .output()
        .unwrap();
    assert!(metrics.status.success());
    let report = String::from_utf8_lossy(&metrics.stdout);
    assert!(report.contains("requests including pool rust"));
    assert!(report.contains("(1 intervals)"));
    assert!(report.contains("observed runner-minutes: 0.00"));
    posts.store(0, Ordering::SeqCst);
    // A readable branch-rules list with a withheld bypass list may still
    // schedule CI, but must never become a complete landing observation.
    mode.store(0, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let hidden_db = data.path().join("hidden-bypass.sqlite");
    let mut hidden_inbox = Inbox::open(&hidden_db).unwrap();
    let mut hidden_store = Store::open(&hidden_db).unwrap();
    hidden_inbox
        .record("hidden", "pull_request", &[9; 32], 42, 7)
        .unwrap();
    let hidden_event = hidden_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut hidden_store,
        &hidden_event,
        &fixture_policy,
    )
    .await
    .unwrap();
    hidden_store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    hidden_store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    hidden_store.start_integration(id).unwrap();
    hidden_store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    let one_host: sluss::scheduler::Capacity =
        toml::from_str(&std::fs::read_to_string(&capacity).unwrap()).unwrap();
    mode.store(67, Ordering::SeqCst);
    assert!(matches!(
        sluss::dispatch::batch(
            &hidden_db,
            &fixture_app,
            &fixture_policy,
            &one_host,
            &settings,
            id
        )
        .await
        .unwrap(),
        sluss::dispatch::Outcome::Sent { run: 123, .. }
    ));
    let hidden_conn = rusqlite::Connection::open(&hidden_db).unwrap();
    let (has_rules,has_security):(bool,bool) = hidden_conn.query_row("SELECT EXISTS(SELECT 1 FROM dispatch_branch_rules WHERE id=?1), EXISTS(SELECT 1 FROM dispatch_branch_security WHERE id=?1)",[id],|r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert!(has_rules && !has_security);
    sluss::worker::observe_dispatches_once(&fixture_app, &hidden_db, &one_host, &fixture_policy)
        .await
        .unwrap();
    let mut hidden_journal = sluss::landing::LandingJournal::open(&hidden_db).unwrap();
    assert!(matches!(
        hidden_journal.prepare_observed_batch("hidden-land", id, "R_repo"),
        Err(sluss::landing::LandingError::StaleBatch)
    ));
    posts.store(0, Ordering::SeqCst);
    // A stable snapshot is still insufficient if a different App is the
    // visible bypass actor. The signed App identity must reject the claim.
    mode.store(0, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let foreign_db = data.path().join("foreign-bypass.sqlite");
    let mut foreign_inbox = Inbox::open(&foreign_db).unwrap();
    let mut foreign_store = Store::open(&foreign_db).unwrap();
    foreign_inbox
        .record("foreign", "pull_request", &[10; 32], 42, 7)
        .unwrap();
    let foreign_event = foreign_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut foreign_store,
        &foreign_event,
        &fixture_policy,
    )
    .await
    .unwrap();
    foreign_store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    foreign_store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    foreign_store.start_integration(id).unwrap();
    foreign_store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    mode.store(77, Ordering::SeqCst);
    assert!(matches!(
        sluss::dispatch::batch(
            &foreign_db,
            &fixture_app,
            &fixture_policy,
            &one_host,
            &settings,
            id
        )
        .await
        .unwrap(),
        sluss::dispatch::Outcome::Sent { run: 123, .. }
    ));
    sluss::worker::observe_dispatches_once(&fixture_app, &foreign_db, &one_host, &fixture_policy)
        .await
        .unwrap();
    let trust = sluss::landing::TrustedCiPolicy {
        version: 1,
        installation_id: 42,
        repository_id: 7,
        repository_node_id: "R_repo".into(),
        target_ref: "refs/heads/main".into(),
        repository_policy_digest: sluss::reconcile::policy_digest(&fixture_app, &fixture_policy)
            .unwrap(),
        policy_digest: "e".repeat(64),
        workflow_id: 17,
        workflow_path: ".github/workflows/ci.yml".into(),
        required_jobs: ["job-1-0".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
        runner_ids: [1].into(),
    };
    let mut foreign_journal = sluss::landing::LandingJournal::open(&foreign_db).unwrap();
    foreign_journal
        .prepare_trusted_batch("foreign-land", id, &trust)
        .unwrap();
    assert!(matches!(
        fixture_app
            .refresh_and_claim_observed_landing(
                &mut foreign_journal,
                &fixture_token,
                &security_token,
                &fixture_policy,
                &trust,
                "foreign-land"
            )
            .await,
        Err(sluss::github::LandingRefreshError::UnsafeBranchPolicy)
    ));
    assert_eq!(
        foreign_journal.get("foreign-land").unwrap().unwrap().1,
        sluss::landing::IntentState::Prepared
    );
    // A recovered candidate can race with a later direct receipt. Preserve
    // both identities, cancel each at most once, and keep the reservation.
    mode.store(0, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let conflict_db = data.path().join("dispatch-conflict.sqlite");
    let mut conflict_inbox = Inbox::open(&conflict_db).unwrap();
    let mut conflict_store = Store::open(&conflict_db).unwrap();
    conflict_inbox
        .record("conflict", "pull_request", &[99; 32], 42, 7)
        .unwrap();
    let event = conflict_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut conflict_store,
        &event,
        &fixture_policy,
    )
    .await
    .unwrap();
    conflict_store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    conflict_store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    conflict_store.start_integration(id).unwrap();
    conflict_store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    let mut conflict_scheduler =
        sluss::scheduler::Scheduler::open(&conflict_db, one_host.clone()).unwrap();
    mode.store(123, Ordering::SeqCst);
    posts.store(0, Ordering::SeqCst);
    let (sent, recovered) = tokio::join!(
        sluss::dispatch::batch(
            &conflict_db,
            &fixture_app,
            &fixture_policy,
            &one_host,
            &settings,
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while posts.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let result = fixture_app
                .recover_uncertain_dispatch(
                    &mut conflict_scheduler,
                    &fixture_token,
                    "owner",
                    "repo",
                    id,
                    123,
                )
                .await;
            mode.store(0, Ordering::SeqCst);
            result
        }
    );
    assert!(recovered.is_ok(), "{recovered:?}");
    assert!(matches!(
        sent,
        Err(sluss::dispatch::Error::Dispatch(
            sluss::github::DispatchError::Scheduler(
                sluss::scheduler::SchedulerError::DispatchReceiptConflict
            )
        ))
    ));
    let report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&conflict_db)
        .args(["dispatch-conflict", "--batch-id", id, "--capacity"])
        .arg(&capacity)
        .output()
        .unwrap();
    assert!(report.status.success());
    assert!(
        String::from_utf8_lossy(&report.stdout)
            .contains("recovered run 123, direct receipt run 124")
    );
    let write_token = fixture_app.dispatch_token(42, 7).await.unwrap();
    posts.store(0, Ordering::SeqCst);
    assert!(
        fixture_app
            .cancel_conflicting_run(
                &mut conflict_scheduler,
                &write_token,
                "owner",
                "repo",
                id,
                125
            )
            .await
            .is_err()
    );
    mode.store(23, Ordering::SeqCst);
    assert!(matches!(
        fixture_app
            .cancel_conflicting_run(
                &mut conflict_scheduler,
                &write_token,
                "owner",
                "repo",
                id,
                123
            )
            .await,
        Err(sluss::github::DispatchError::RunMismatch)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    mode.store(31, Ordering::SeqCst);
    let mut competitor = sluss::scheduler::Scheduler::open(&conflict_db, one_host.clone()).unwrap();
    let (first_cancel, second_cancel) = tokio::join!(
        fixture_app.cancel_conflicting_run(
            &mut conflict_scheduler,
            &write_token,
            "owner",
            "repo",
            id,
            123
        ),
        fixture_app.cancel_conflicting_run(&mut competitor, &write_token, "owner", "repo", id, 123),
    );
    assert_eq!(
        [&first_cancel, &second_cancel]
            .iter()
            .filter(|r| matches!(r, Ok(sluss::github::StaleCancellation::Requested)))
            .count(),
        1
    );
    assert!([first_cancel, second_cancel].iter().all(|r| matches!(
        r,
        Ok(sluss::github::StaleCancellation::Requested
            | sluss::github::StaleCancellation::AlreadyRequested
            | sluss::github::StaleCancellation::Uncertain)
    )));
    assert!(matches!(
        fixture_app
            .cancel_conflicting_run(
                &mut conflict_scheduler,
                &write_token,
                "owner",
                "repo",
                id,
                123
            )
            .await,
        Ok(sluss::github::StaleCancellation::AlreadyRequested)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    mode.store(32, Ordering::SeqCst);
    let db_arg = conflict_db.clone();
    let config_arg = config.clone();
    let capacity_arg = capacity.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let cancel_cli = move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(&db_arg)
            .args([
                "cancel-conflict",
                "--batch-id",
                id,
                "--run-id",
                "124",
                "--config",
            ])
            .arg(&config_arg)
            .arg("--capacity")
            .arg(&capacity_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", &key_arg)
            .output()
            .unwrap()
    };
    let output = tokio::task::spawn_blocking(cancel_cli).await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Uncertain"));
    drop(conflict_scheduler);
    let mut restarted = sluss::scheduler::Scheduler::open(&conflict_db, one_host.clone()).unwrap();
    assert!(matches!(
        fixture_app
            .cancel_conflicting_run(&mut restarted, &write_token, "owner", "repo", id, 124)
            .await,
        Ok(sluss::github::StaleCancellation::Uncertain)
    ));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        2,
        "ambiguous cancellation must not be resent"
    );
    assert!(restarted.release_invalidated_ci_only(id, 123).is_err());
    assert_eq!(restarted.pending_dispatches().unwrap().len(), 1);
    let inventory_token = fixture_app.runner_inventory_token(42, 7).await.unwrap();
    mode.store(31, Ordering::SeqCst);
    assert!(matches!(
        fixture_app
            .resolve_dispatch_conflict(
                &mut restarted,
                &fixture_token,
                &inventory_token,
                ("owner", "repo"),
                id,
                &one_host
            )
            .await,
        Ok(sluss::github::ConflictResolution::Waiting)
    ));
    for bad_mode in [21, 23, 41, 51, 104, 239] {
        mode.store(bad_mode, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        assert!(
            fixture_app
                .resolve_dispatch_conflict(
                    &mut restarted,
                    &fixture_token,
                    &inventory_token,
                    ("owner", "repo"),
                    id,
                    &one_host
                )
                .await
                .is_err(),
            "mode {bad_mode}"
        );
        assert_eq!(restarted.pending_dispatches().unwrap().len(), 1);
    }
    mode.store(0, Ordering::SeqCst);
    let collision_db = rusqlite::Connection::open(&conflict_db).unwrap();
    collision_db.execute("INSERT INTO dispatch_receipt_conflicts(id,recovered_run,direct_run) VALUES('other','124','999')", []).unwrap();
    assert!(
        fixture_app
            .resolve_dispatch_conflict(
                &mut restarted,
                &fixture_token,
                &inventory_token,
                ("owner", "repo"),
                id,
                &one_host
            )
            .await
            .is_err()
    );
    assert_eq!(restarted.pending_dispatches().unwrap().len(), 1);
    collision_db
        .execute(
            "DELETE FROM dispatch_receipt_conflicts WHERE id='other'",
            [],
        )
        .unwrap();
    // A consistent copy simulates a restart with the opted-in release worker.
    let cancelled_db = data.path().join("conflict-cancelled.sqlite");
    collision_db
        .execute("VACUUM INTO ?1", [cancelled_db.to_str().unwrap()])
        .unwrap();
    mode.store(43, Ordering::SeqCst);
    assert_eq!(
        sluss::worker::release_ci_only_once(
            &fixture_app,
            &cancelled_db,
            &one_host,
            &fixture_policy
        )
        .await
        .unwrap(),
        1
    );
    let cancelled_scheduler =
        sluss::scheduler::Scheduler::open(&cancelled_db, one_host.clone()).unwrap();
    assert!(cancelled_scheduler.conflict_release_recorded(id).unwrap());
    mode.store(0, Ordering::SeqCst);
    let worker_db = data.path().join("conflict-worker.sqlite");
    collision_db
        .execute("VACUUM INTO ?1", [worker_db.to_str().unwrap()])
        .unwrap();
    assert_eq!(
        sluss::worker::release_ci_only_once(&fixture_app, &worker_db, &one_host, &fixture_policy)
            .await
            .unwrap(),
        1
    );
    // Upgrade a database made before topology history was introduced.
    let legacy_capacity_db = rusqlite::Connection::open(&worker_db).unwrap();
    legacy_capacity_db
        .execute("DROP TABLE capacity_release_topology_history", [])
        .unwrap();
    drop(legacy_capacity_db);
    let worker_scheduler = sluss::scheduler::Scheduler::open(&worker_db, one_host.clone()).unwrap();
    assert!(worker_scheduler.conflict_release_recorded(id).unwrap());
    assert!(worker_scheduler.pending_dispatches().unwrap().is_empty());
    // A topology expansion retains independently recorded historical conflict
    // evidence under its original release envelope, including after restart.
    let migration_store = Store::open(&worker_db).unwrap();
    migration_store.set_paused(true).unwrap();
    let capacity_digest = sluss::scheduler::Scheduler::capacity_digest(&worker_db)
        .unwrap()
        .unwrap();
    let new_capacity = data.path().join("expanded-capacity.toml");
    std::fs::write(&new_capacity, "[pools]\nrust=2\n[hosts.sluss-host-test]\nslots=1\nrunner_ids=[1]\n[hosts.sluss-host-test.pools]\nrust=1\n[hosts.sluss-host-new]\nslots=1\nrunner_ids=[2]\n[hosts.sluss-host-new.pools]\nrust=1\n").unwrap();
    let migration = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&worker_db)
        .args([
            "migrate-capacity",
            "--expected-digest",
            &capacity_digest,
            "--config",
        ])
        .arg(&config)
        .arg("--capacity")
        .arg(&new_capacity)
        .output()
        .unwrap();
    assert!(
        migration.status.success(),
        "{}",
        String::from_utf8_lossy(&migration.stderr)
    );
    assert!(worker_scheduler.conflict_release_recorded(id).unwrap());
    let expanded: sluss::scheduler::Capacity =
        toml::from_str(&std::fs::read_to_string(&new_capacity).unwrap()).unwrap();
    let expanded_scheduler = sluss::scheduler::Scheduler::open(&worker_db, expanded).unwrap();
    assert!(expanded_scheduler.conflict_release_recorded(id).unwrap());
    assert!(migration_store.paused().unwrap());
    let mut competitor = sluss::scheduler::Scheduler::open(&conflict_db, one_host.clone()).unwrap();
    let (a, b) = tokio::join!(
        fixture_app.resolve_dispatch_conflict(
            &mut restarted,
            &fixture_token,
            &inventory_token,
            ("owner", "repo"),
            id,
            &one_host
        ),
        fixture_app.resolve_dispatch_conflict(
            &mut competitor,
            &fixture_token,
            &inventory_token,
            ("owner", "repo"),
            id,
            &one_host
        )
    );
    assert!(
        [&a, &b]
            .iter()
            .any(|r| matches!(r, Ok(sluss::github::ConflictResolution::Resolved))),
        "{a:?} {b:?}"
    );
    assert!(
        [&a, &b].iter().all(|r| matches!(
            r,
            Ok(sluss::github::ConflictResolution::Resolved
                | sluss::github::ConflictResolution::AlreadyResolved)
        )),
        "{a:?} {b:?}"
    );
    assert!(restarted.pending_dispatches().unwrap().is_empty());
    drop(restarted);
    let restarted = sluss::scheduler::Scheduler::open(&conflict_db, one_host.clone()).unwrap();
    assert!(restarted.conflict_release_recorded(id).unwrap());
    assert_eq!(restarted.receipt_conflict(id).unwrap(), Some((123, 124)));
    let report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&conflict_db)
        .args(["dispatch-conflict", "--batch-id", id, "--capacity"])
        .arg(&capacity)
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&report.stdout).contains("evidence is historical"));
    assert!(collision_db.execute("INSERT INTO dispatch_intents(id,plan,state,run_id) VALUES('future','{}','observed','124')", []).is_err());
    let db_arg = conflict_db.clone();
    let config_arg = config.clone();
    let capacity_arg = capacity.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args(["resolve-dispatch-conflict", "--batch-id", id, "--config"])
            .arg(config_arg)
            .arg("--capacity")
            .arg(capacity_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("AlreadyResolved"));
    collision_db
        .execute(
            "UPDATE dispatch_conflict_releases SET digest='corrupted' WHERE id=?1",
            [id],
        )
        .unwrap();
    assert!(restarted.conflict_release_recorded(id).is_err());
    posts.store(0, Ordering::SeqCst);
    let check_token = fixture_app.failed_check_token(42, 7).await.unwrap();
    for check_mode in [40, 95, 96, 97, 98, 99, 103, 129] {
        mode.store(0, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        let failure_db = data
            .path()
            .join(format!("failed-check-{check_mode}.sqlite"));
        let mut failure_inbox = Inbox::open(&failure_db).unwrap();
        let mut failure_store = Store::open(&failure_db).unwrap();
        failure_inbox
            .record("failure", "pull_request", &[11; 32], 42, 7)
            .unwrap();
        let event = failure_inbox.pending().unwrap().remove(0);
        sluss::reconcile::reconcile_once(
            &fixture_app,
            &fixture_token,
            &mut failure_store,
            &event,
            &fixture_policy,
        )
        .await
        .unwrap();
        failure_store
            .plan_batch(id, &"b".repeat(40), 1, 1)
            .unwrap()
            .unwrap();
        failure_store
            .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
            .unwrap();
        failure_store.start_integration(id).unwrap();
        failure_store
            .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
            .unwrap();
        mode.store(check_mode, Ordering::SeqCst);
        assert!(matches!(
            sluss::dispatch::batch(
                &failure_db,
                &fixture_app,
                &fixture_policy,
                &one_host,
                &settings,
                id
            )
            .await
            .unwrap(),
            sluss::dispatch::Outcome::Sent { run: 123, .. }
        ));
        sluss::worker::observe_dispatches_once(
            &fixture_app,
            &failure_db,
            &one_host,
            &fixture_policy,
        )
        .await
        .unwrap();
        posts.store(0, Ordering::SeqCst);
        let mut first = sluss::scheduler::Scheduler::open(&failure_db, one_host.clone()).unwrap();
        let mut second = sluss::scheduler::Scheduler::open(&failure_db, one_host.clone()).unwrap();
        if check_mode == 129 {
            let _journal = sluss::landing::LandingJournal::open(&failure_db).unwrap();
            let conn = rusqlite::Connection::open(&failure_db).unwrap();
            conn.execute("INSERT INTO pending_ci_checks_v1(batch_id,body,state,check_id,suite_id,direct,recovered) VALUES('other','{}','created','444','44',1,0)", []).unwrap();
        }
        assert!(matches!(
            fixture_app
                .publish_failed_check(
                    &mut first,
                    (&fixture_token, &fixture_token),
                    "owner",
                    "repo",
                    id
                )
                .await,
            Err(sluss::github::FailureCheckError::Scope)
        ));
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        if check_mode == 40 {
            failure_inbox
                .record("pending-change", "pull_request_review", &[12; 32], 42, 7)
                .unwrap();
            assert!(
                fixture_app
                    .publish_failed_check(
                        &mut first,
                        (&fixture_token, &check_token),
                        "owner",
                        "repo",
                        id
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                posts.load(Ordering::SeqCst),
                0,
                "pending review refresh must stop the POST"
            );
            let pending = failure_inbox.pending().unwrap().remove(0);
            sluss::reconcile::reconcile_once(
                &fixture_app,
                &fixture_token,
                &mut failure_store,
                &pending,
                &fixture_policy,
            )
            .await
            .unwrap();
        }
        if check_mode == 98 {
            let db_arg = failure_db.clone();
            let config_arg = config.clone();
            let capacity_arg = capacity.clone();
            let key_arg = key_dir.path().join("test-key.pem");
            let output = tokio::task::spawn_blocking(move || {
                Command::new(env!("CARGO_BIN_EXE_sluss"))
                    .arg("--db")
                    .arg(db_arg)
                    .args(["report-failed-check", "--batch-id", id, "--config"])
                    .arg(config_arg)
                    .arg("--capacity")
                    .arg(capacity_arg)
                    .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("Created(444)"));
        } else if matches!(check_mode, 99 | 103) {
            let (sent, recovered) = tokio::join!(
                fixture_app.publish_failed_check(
                    &mut first,
                    (&fixture_token, &check_token),
                    "owner",
                    "repo",
                    id
                ),
                async {
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while posts.load(Ordering::SeqCst) == 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .unwrap();
                    let outcome = fixture_app
                        .recover_failed_check(&mut second, &fixture_token, "owner", "repo", id, 444)
                        .await;
                    mode.store(100, Ordering::SeqCst);
                    outcome
                }
            );
            assert!(matches!(
                recovered,
                Ok(sluss::github::FailureCheckOutcome::Recovered(444))
            ));
            if check_mode == 99 {
                assert!(matches!(
                    sent,
                    Err(sluss::github::FailureCheckError::Uncertain)
                ));
                assert!(matches!(
                    first.failure_check(id).unwrap().unwrap().1,
                    sluss::scheduler::FailureCheckState::Conflict {
                        check_id: 444,
                        other_check_id: 445,
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    sent,
                    Ok(sluss::github::FailureCheckOutcome::Created(444))
                ));
                assert_eq!(first.failure_check_source(id).unwrap(), Some((true, true)));
            }
        } else {
            let (a, b) = tokio::join!(
                fixture_app.publish_failed_check(
                    &mut first,
                    (&fixture_token, &check_token),
                    "owner",
                    "repo",
                    id
                ),
                fixture_app.publish_failed_check(
                    &mut second,
                    (&fixture_token, &check_token),
                    "owner",
                    "repo",
                    id
                ),
            );
            assert_eq!(
                posts.load(Ordering::SeqCst),
                1,
                "one POST for mode {check_mode}"
            );
            if check_mode == 40 {
                assert_eq!(
                    [&a, &b]
                        .iter()
                        .filter(|result| matches!(
                            result,
                            Ok(sluss::github::FailureCheckOutcome::Created(444))
                        ))
                        .count(),
                    1
                );
            } else {
                assert!(
                    a.is_err() && b.is_err(),
                    "ambiguous result cannot be called created"
                );
            }
        }
        drop(first);
        drop(second);
        let mut restarted =
            sluss::scheduler::Scheduler::open(&failure_db, one_host.clone()).unwrap();
        let recovery = fixture_app
            .publish_failed_check(
                &mut restarted,
                (&fixture_token, &check_token),
                "owner",
                "repo",
                id,
            )
            .await;
        if matches!(check_mode, 40 | 98 | 103) {
            assert!(matches!(
                recovery,
                Ok(sluss::github::FailureCheckOutcome::AlreadyCreated(444))
            ));
        } else {
            assert!(matches!(
                recovery,
                Err(sluss::github::FailureCheckError::Uncertain)
            ));
        }
        assert_eq!(
            posts.load(Ordering::SeqCst),
            1,
            "restart must not resend for mode {check_mode}"
        );
        if check_mode == 40 {
            assert!(
                fixture_app
                    .recover_failed_check(&mut restarted, &fixture_token, "owner", "repo", id, 445)
                    .await
                    .is_err()
            );
            assert!(matches!(
                restarted.failure_check(id).unwrap().unwrap().1,
                sluss::scheduler::FailureCheckState::Created { check_id: 444, .. }
            ));
        }
        if check_mode == 95 {
            for mismatch in [96, 97, 101, 102] {
                mode.store(mismatch, Ordering::SeqCst);
                assert!(matches!(
                    fixture_app
                        .recover_failed_check(
                            &mut restarted,
                            &fixture_token,
                            "owner",
                            "repo",
                            id,
                            444
                        )
                        .await,
                    Err(sluss::github::FailureCheckError::CandidateMismatch)
                ));
                assert!(matches!(
                    restarted.failure_check(id).unwrap().unwrap().1,
                    sluss::scheduler::FailureCheckState::Sending { .. }
                ));
            }
            mode.store(95, Ordering::SeqCst);
            let db_arg = failure_db.clone();
            let config_arg = config.clone();
            let capacity_arg = capacity.clone();
            let key_arg = key_dir.path().join("test-key.pem");
            let output = tokio::task::spawn_blocking(move || {
                Command::new(env!("CARGO_BIN_EXE_sluss"))
                    .arg("--db")
                    .arg(db_arg)
                    .args([
                        "recover-failed-check",
                        "--batch-id",
                        id,
                        "--check-run-id",
                        "444",
                        "--config",
                    ])
                    .arg(config_arg)
                    .arg("--capacity")
                    .arg(capacity_arg)
                    .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("does not prove request origin")
            );
            assert!(matches!(
                fixture_app
                    .publish_failed_check(
                        &mut restarted,
                        (&fixture_token, &check_token),
                        "owner",
                        "repo",
                        id
                    )
                    .await,
                Ok(sluss::github::FailureCheckOutcome::Recovered(444))
            ));
            assert_eq!(
                restarted.failure_check_source(id).unwrap(),
                Some((false, true))
            );
            assert_eq!(posts.load(Ordering::SeqCst), 1);
        }
    }
    posts.store(0, Ordering::SeqCst);
    // A previously prepared intent is pinned to a now-busy host. A second
    // batch must still reach the other idle host, without resending the first.
    mode.store(46, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let queue_db = data.path().join("no-head-of-line.sqlite");
    let mut queue_inbox = Inbox::open(&queue_db).unwrap();
    let mut queue_store = Store::open(&queue_db).unwrap();
    queue_inbox
        .record("queue", "pull_request", &[8; 32], 42, 7)
        .unwrap();
    let queue_event = queue_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut queue_store,
        &queue_event,
        &fixture_policy,
    )
    .await
    .unwrap();
    let pinned_id = "batch-0000000000000000";
    for batch_id in [pinned_id, id] {
        queue_store
            .plan_batch(batch_id, &"b".repeat(40), 1, 2)
            .unwrap()
            .unwrap();
        queue_store
            .bind_policy_paths(batch_id, &[".github/workflows/ci.yml".into()])
            .unwrap();
        queue_store.start_integration(batch_id).unwrap();
        queue_store
            .finish_integration(batch_id, &"a".repeat(40), &"e".repeat(64))
            .unwrap();
    }
    let plan = sluss::scheduler::DispatchPlan {
        installation_id: 42,
        repository_id: 7,
        workflow_id: 17,
        integration_sha: "a".repeat(40),
        staging_ref: format!("refs/heads/sluss/{pinned_id}"),
        policy_digest: "e".repeat(64),
        required_jobs: ["job-1-0".into()].into(),
        suites: ["full".into()].into(),
        selection_policy_digest: None,
    };
    let mut queue_scheduler =
        sluss::scheduler::Scheduler::open(&queue_db, independent_capacity.clone()).unwrap();
    let original = queue_scheduler
        .reserve_batch_dispatch_on(
            pinned_id,
            &settings.request,
            &plan,
            &["sluss-host-busy".into()].into(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(original.host, "sluss-host-busy");
    mode.store(47, Ordering::SeqCst);
    assert_eq!(
        sluss::worker::dispatch_once(
            &fixture_app,
            &queue_db,
            &fixture_policy,
            &independent_capacity,
            &settings
        )
        .await
        .unwrap(),
        Some(sluss::dispatch::Outcome::NoCapacity)
    );
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    mode.store(46, Ordering::SeqCst);
    assert_eq!(
        sluss::worker::dispatch_once(
            &fixture_app,
            &queue_db,
            &fixture_policy,
            &independent_capacity,
            &settings
        )
        .await
        .unwrap(),
        Some(sluss::dispatch::Outcome::Sent {
            run: 123,
            host: "sluss-host-test".into()
        })
    );
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let pending = queue_scheduler.pending_dispatches().unwrap();
    assert!(pending.iter().any(|item| item.reservation.id == pinned_id
        && item.state == sluss::scheduler::DispatchState::Prepared));
    assert!(pending.iter().any(|item| item.reservation.id == id
        && item.state == (sluss::scheduler::DispatchState::Accepted { run_id: 123 })));
    posts.store(0, Ordering::SeqCst);
    mode.store(0, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    assert!(matches!(
        sluss::coordinator::prepare_github_from_provider(
            &db,
            &fixture_app,
            &fixture_policy,
            id,
            &[".github/workflows/ci.yml".into()],
            None,
        )
        .await,
        Err(sluss::coordinator::ProviderPrepareError::UnsupportedProvider)
    ));
    assert_eq!(
        store.batch(id).unwrap().unwrap().0.state,
        sluss::batch::State::Planned
    );
    store
        .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
        .unwrap();
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    let policy = data.path().join("verification.toml");
    let policy_text = include_str!("../examples/verification.toml");
    std::fs::write(&policy, policy_text).unwrap();
    store
        .record_selection(
            id,
            &FrozenSelection {
                base_sha: "b".repeat(40),
                integration_sha: "a".repeat(40),
                policy_digest: Policy::load(&policy).unwrap().digest().unwrap(),
                selection: Selection::Selected {
                    suites: ["api-tests".into(), "ui-tests".into()].into(),
                    components: ["api".into(), "ui".into()].into(),
                },
            },
        )
        .unwrap();
    drop(store);
    let launch = |args: Vec<String>| {
        let db = db.clone();
        let key = key_dir.path().join("test-key.pem");
        tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_sluss"))
                .arg("--db")
                .arg(db)
                .args(args)
                .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
                .output()
                .unwrap()
        })
    };
    let dispatch_args = || {
        vec![
            "dispatch-github".into(),
            "--batch-id".into(),
            id.into(),
            "--config".into(),
            config.to_str().unwrap().into(),
            "--capacity".into(),
            capacity.to_str().unwrap().into(),
            "--pool".into(),
            "rust=1".into(),
            "--workflow-id".into(),
            "17".into(),
            "--required-job".into(),
            "job-1-0".into(),
        ]
    };
    let missing = launch(dispatch_args()).await.unwrap();
    assert!(!missing.status.success());
    let mut changed_args = dispatch_args();
    changed_args.extend([
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ]);
    std::fs::write(
        &policy,
        policy_text.replace(
            "always_full = [\"ci/\"]",
            "always_full = [\"ci/\", \"scripts/\"]",
        ),
    )
    .unwrap();
    let changed = launch(changed_args).await.unwrap();
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("verification policy changed"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    std::fs::write(&policy, policy_text).unwrap();
    // Simulate a selection frozen by the previous algorithm version. It must
    // not be dispatched even when the operator's TOML is unchanged.
    let conn = rusqlite::Connection::open(&db).unwrap();
    let body: String = conn
        .query_row(
            "SELECT body FROM batch_selection WHERE batch_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    let mut legacy: FrozenSelection = serde_json::from_str(&body).unwrap();
    {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"sluss-selective-policy-v1\0");
        hash.update(serde_json::to_vec(&Policy::load(&policy).unwrap()).unwrap());
        legacy.policy_digest = hex::encode(hash.finalize());
    }
    conn.execute(
        "UPDATE batch_selection SET body=?2 WHERE batch_id=?1",
        rusqlite::params![id, serde_json::to_string(&legacy).unwrap()],
    )
    .unwrap();
    let mut legacy_args = dispatch_args();
    legacy_args.extend([
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ]);
    let rejected = launch(legacy_args).await.unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("verification policy changed"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    conn.execute(
        "UPDATE batch_selection SET body=?2 WHERE batch_id=?1",
        rusqlite::params![id, body],
    )
    .unwrap();
    reads.store(0, Ordering::SeqCst);
    mode.store(30, Ordering::SeqCst);
    let mut changed_base_args = dispatch_args();
    changed_base_args.extend([
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ]);
    let changed_base = launch(changed_base_args.clone()).await.unwrap();
    assert!(!changed_base.status.success());
    assert!(
        String::from_utf8_lossy(&changed_base.stderr).contains("Unstable"),
        "{}",
        String::from_utf8_lossy(&changed_base.stderr)
    );
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    reads.store(0, Ordering::SeqCst);
    mode.store(29, Ordering::SeqCst);
    let unstable = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(!unstable.status.success());
    assert!(String::from_utf8_lossy(&unstable.stderr).contains("UnstableObservation"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    reads.store(0, Ordering::SeqCst);
    mode.store(28, Ordering::SeqCst);
    let rejected = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("RunnerMismatch"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    reads.store(0, Ordering::SeqCst);
    mode.store(37, Ordering::SeqCst);
    let moved_after_scan = launch(changed_base_args).await.unwrap();
    assert!(!moved_after_scan.status.success());
    assert!(String::from_utf8_lossy(&moved_after_scan.stderr).contains("StaleBase"));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    mode.store(33, Ordering::SeqCst);
    let output = launch(vec![
        "dispatch-github".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--pool".into(),
        "rust=1".into(),
        "--workflow-id".into(),
        "17".into(),
        "--required-job".into(),
        "job-1-0".into(),
        "--verification-policy".into(),
        policy.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("dispatched run 123 to reserved host sluss-host-test")
    );
    mode.store(0, Ordering::SeqCst);
    // A fresh scan must remove a PR whose reviewer no longer has write access.
    let second_db = data.path().join("revoked.sqlite");
    let mut second_inbox = Inbox::open(&second_db).unwrap();
    let mut second_store = Store::open(&second_db).unwrap();
    second_inbox
        .record("initial-revoked", "pull_request", &[42; 32], 42, 7)
        .unwrap();
    let second_event = second_inbox.pending().unwrap().remove(0);
    sluss::reconcile::reconcile_once(
        &fixture_app,
        &fixture_token,
        &mut second_store,
        &second_event,
        &fixture_policy,
    )
    .await
    .unwrap();
    second_store
        .plan_batch(id, &"b".repeat(40), 1, 1)
        .unwrap()
        .unwrap();
    second_store.start_integration(id).unwrap();
    second_store
        .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
        .unwrap();
    mode.store(38, Ordering::SeqCst);
    let args = dispatch_args();
    let key = key_dir.path().join("test-key.pem");
    let path = second_db.clone();
    let revoked = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(path)
            .args(args)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!revoked.status.success());
    assert!(matches!(
        second_store.batch(id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    assert!(second_store.candidates().unwrap().is_empty());
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    mode.store(0, Ordering::SeqCst);
    let observer_app =
        App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let observer_capacity: sluss::scheduler::Capacity =
        toml::from_str(&std::fs::read_to_string(&capacity).unwrap()).unwrap();
    let observer_policy = sluss::config::Config::load(&config).unwrap().policy();
    for bad_mode in [51, 239] {
        mode.store(bad_mode, Ordering::SeqCst);
        assert!(
            sluss::worker::observe_dispatches_once(
                &observer_app,
                &db,
                &observer_capacity,
                &observer_policy
            )
            .await
            .is_err()
        );
    }
    let frozen = sluss::scheduler::Scheduler::open(&db, observer_capacity.clone()).unwrap();
    assert_eq!(
        frozen.dispatch_workflow(id).unwrap().as_deref(),
        Some(".github/workflows/ci.yml")
    );
    assert!(matches!(
        frozen.pending_dispatches().unwrap()[0].state,
        sluss::scheduler::DispatchState::Accepted { run_id: 123 }
    ));
    mode.store(23, Ordering::SeqCst);
    assert!(
        sluss::worker::observe_dispatches_once(
            &observer_app,
            &db,
            &observer_capacity,
            &observer_policy,
        )
        .await
        .is_err()
    );
    mode.store(0, Ordering::SeqCst);
    sluss::worker::observe_dispatches_once(
        &observer_app,
        &db,
        &observer_capacity,
        &observer_policy,
    )
    .await
    .unwrap();
    let before_posts = posts.load(Ordering::SeqCst);
    let mut no_success_publisher =
        sluss::scheduler::Scheduler::open(&db, observer_capacity.clone()).unwrap();
    assert!(matches!(
        observer_app
            .publish_failed_check(
                &mut no_success_publisher,
                (&fixture_token, &check_token),
                "owner",
                "repo",
                id
            )
            .await,
        Err(sluss::github::FailureCheckError::Scheduler(
            sluss::scheduler::SchedulerError::FailureCheckConflict
        ))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_posts);
    // A fresh provider read must match the frozen run, including the latest
    // attempt and repository node identity, before this experimental claim.
    let mut landing = sluss::landing::LandingJournal::open(&db).unwrap();
    let mut selected_trust = trust.clone();
    selected_trust.suites = ["api-tests".into(), "ui-tests".into()].into();
    selected_trust.selection_policy_digest = Some(Policy::load(&policy).unwrap().digest().unwrap());
    let mut preview = sluss::landing::LandingJournal::open_read_only(&db).unwrap();
    mode.store(0, Ordering::SeqCst);
    assert_eq!(
        observer_app
            .preview_trusted_batch(
                &mut preview,
                &fixture_token,
                &security_token,
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .unwrap()
            .staged_sha,
        "a".repeat(40)
    );
    let trust_file = data.path().join("trusted-ci.toml");
    std::fs::write(&trust_file, toml::to_string(&selected_trust).unwrap()).unwrap();
    let mut candidate_original = sluss::landing::LandingJournal::open(&db).unwrap();
    let original = observer_app
        .verify_staged_ci(
            &mut candidate_original,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id,
        )
        .await
        .unwrap();
    for invalid in [13, 16, 21, 22, 40, 63, 64, 67, 77, 104, 110, 117] {
        mode.store(invalid, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        graphql_counts.store(0, Ordering::SeqCst);
        assert!(
            observer_app
                .verify_staged_ci(
                    &mut candidate_original,
                    (&fixture_token, &security_token),
                    &observer_policy,
                    &selected_trust,
                    id
                )
                .await
                .is_err(),
            "mode {invalid}"
        );
    }
    mode.store(0, Ordering::SeqCst);
    assert_eq!(
        candidate_original
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(original.clone())
    );
    let candidate_race_db = data.path().join("ci-evidence-race.sqlite");
    let race_conn = rusqlite::Connection::open(&db).unwrap();
    race_conn
        .execute("VACUUM INTO ?1", [candidate_race_db.to_str().unwrap()])
        .unwrap();
    let mut race_journal = sluss::landing::LandingJournal::open(&candidate_race_db).unwrap();
    let mut race_inbox = Inbox::open(&candidate_race_db).unwrap();
    mode.store(121, Ordering::SeqCst);
    graphql_counts.store(0, Ordering::SeqCst);
    let (raced, _) = tokio::join!(
        observer_app.verify_staged_ci(
            &mut race_journal,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while graphql_counts.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            race_inbox
                .record("ci-candidate-race", "pull_request_review", &[44; 32], 42, 7)
                .unwrap();
            let event = race_inbox.pending().unwrap().remove(0);
            let mut reconciled = Store::open(&candidate_race_db).unwrap();
            sluss::reconcile::reconcile_once(
                &observer_app,
                &fixture_token,
                &mut reconciled,
                &event,
                &observer_policy,
            )
            .await
            .unwrap();
            assert!(race_inbox.pending().unwrap().is_empty());
            mode.store(0, Ordering::SeqCst);
        }
    );
    assert!(
        raced.is_err(),
        "a webhook during the provider scan must reject recording"
    );
    let candidate_race_conn = rusqlite::Connection::open(&candidate_race_db).unwrap();
    let count: i64 = candidate_race_conn
        .query_row("SELECT COUNT(*) FROM ci_evidence_candidates_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 1, "a stale scan must not append new evidence");
    assert_eq!(
        race_journal
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    mode.store(0, Ordering::SeqCst);
    let revalidated = observer_app
        .verify_staged_ci(
            &mut race_journal,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id,
        )
        .await
        .unwrap();
    mode.store(121, Ordering::SeqCst);
    graphql_counts.store(0, Ordering::SeqCst);
    let (periodic_race, _) = tokio::join!(
        observer_app.verify_staged_ci(
            &mut race_journal,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while graphql_counts.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(race_inbox.schedule_if_idle(42, 7).unwrap());
            let event = race_inbox.pending().unwrap().remove(0);
            let mut reconciled = Store::open(&candidate_race_db).unwrap();
            sluss::reconcile::reconcile_once(
                &observer_app,
                &fixture_token,
                &mut reconciled,
                &event,
                &observer_policy,
            )
            .await
            .unwrap();
            assert!(race_inbox.pending().unwrap().is_empty());
            mode.store(0, Ordering::SeqCst);
        }
    );
    assert!(matches!(
        periodic_race,
        Err(sluss::github::LandingRefreshError::Journal(
            sluss::landing::LandingError::StaleBatch
        ))
    ));
    assert_eq!(
        race_journal
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(revalidated)
    );

    let claim_race_db = data.path().join("landing-claim-race.sqlite");
    race_conn
        .execute("VACUUM INTO ?1", [claim_race_db.to_str().unwrap()])
        .unwrap();
    let mut claim_journal = sluss::landing::LandingJournal::open(&claim_race_db).unwrap();
    claim_journal
        .prepare_trusted_batch("land-race", id, &selected_trust)
        .unwrap();
    let mut claim_inbox = Inbox::open(&claim_race_db).unwrap();
    mode.store(121, Ordering::SeqCst);
    graphql_counts.store(0, Ordering::SeqCst);
    let (claim, _) = tokio::join!(
        Box::pin(observer_app.refresh_and_claim_observed_landing(
            &mut claim_journal,
            &fixture_token,
            &security_token,
            &observer_policy,
            &selected_trust,
            "land-race"
        )),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while graphql_counts.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            claim_inbox
                .record("land-race-event", "pull_request_review", &[45; 32], 42, 7)
                .unwrap();
            let event = claim_inbox.pending().unwrap().remove(0);
            let mut reconciled = Store::open(&claim_race_db).unwrap();
            sluss::reconcile::reconcile_once(
                &observer_app,
                &fixture_token,
                &mut reconciled,
                &event,
                &observer_policy,
            )
            .await
            .unwrap();
            assert!(claim_inbox.pending().unwrap().is_empty());
            mode.store(0, Ordering::SeqCst);
        }
    );
    assert!(matches!(
        claim,
        Err(sluss::github::LandingRefreshError::Journal(
            sluss::landing::LandingError::StaleBatch
        ))
    ));
    assert_eq!(
        claim_journal.get("land-race").unwrap().unwrap().1,
        sluss::landing::IntentState::Prepared
    );
    // A CI-only candidate does not require branch bypass and cannot create
    // an App check or claim a landing intent.
    let candidate_db = data.path().join("ci-evidence.sqlite");
    let snapshot_conn = rusqlite::Connection::open(&db).unwrap();
    snapshot_conn
        .execute("VACUUM INTO ?1", [candidate_db.to_str().unwrap()])
        .unwrap();
    let candidate_conn = rusqlite::Connection::open(&candidate_db).unwrap();
    let security_body: String = candidate_conn
        .query_row(
            "SELECT body FROM dispatch_branch_security WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    let mut frozen_security: sluss::github::BranchSecurityObservation =
        serde_json::from_str(&security_body).unwrap();
    frozen_security.rulesets[0].detail["bypass_actors"] = json!([]);
    let security_digest = frozen_security.digest().unwrap();
    candidate_conn
        .execute(
            "UPDATE dispatch_branch_security SET body=?2,digest=?3 WHERE id=?1",
            rusqlite::params![
                id,
                serde_json::to_string(&frozen_security).unwrap(),
                security_digest
            ],
        )
        .unwrap();
    let mut candidate_journal = sluss::landing::LandingJournal::open(&candidate_db).unwrap();
    let mut candidate_preview =
        sluss::landing::LandingJournal::open_read_only(&candidate_db).unwrap();
    mode.store(73, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .preview_trusted_batch(
                &mut candidate_preview,
                &fixture_token,
                &security_token,
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Err(sluss::github::LandingRefreshError::UnsafeBranchPolicy)
    ));
    let writes_before = posts.load(Ordering::SeqCst);
    let recorded = observer_app
        .verify_staged_ci(
            &mut candidate_journal,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id,
        )
        .await
        .unwrap();
    assert_eq!(recorded.candidate.snapshot.run.id, 123);
    assert_eq!(recorded.candidate.plan.staged_sha, "a".repeat(40));
    assert_eq!(recorded.candidate.app_id, 17);
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(recorded.clone())
    );
    let repeat = observer_app
        .verify_staged_ci(
            &mut candidate_journal,
            (&fixture_token, &security_token),
            &observer_policy,
            &selected_trust,
            id,
        )
        .await
        .unwrap();
    assert_eq!(recorded, repeat, "repeated observations must be idempotent");
    assert_eq!(posts.load(Ordering::SeqCst), writes_before);
    let count: i64 = candidate_conn
        .query_row("SELECT COUNT(*) FROM ci_evidence_candidates_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
    candidate_conn
        .execute(
            "UPDATE ci_evidence_candidate_heads_v1 SET digest=?2 WHERE batch_id=?1",
            rusqlite::params![id, original.digest],
        )
        .unwrap();
    assert!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .is_err()
    );
    assert_eq!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .unwrap(),
        recorded
    );
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(recorded.clone())
    );
    let db_arg = candidate_db.clone();
    let config_arg = config.clone();
    let policy_arg = trust_file.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let candidate_cli = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args(["verify-staged-ci", "--batch-id", id, "--config"])
            .arg(config_arg)
            .arg("--policy")
            .arg(policy_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        candidate_cli.status.success(),
        "{}",
        String::from_utf8_lossy(&candidate_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&candidate_cli.stdout).contains("no GitHub check was created"));
    let inspect_cli = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&candidate_db)
        .args(["inspect-staged-ci", "--batch-id", id, "--policy"])
        .arg(&trust_file)
        .output()
        .unwrap();
    assert!(inspect_cli.status.success());
    assert!(String::from_utf8_lossy(&inspect_cli.stdout).contains(&recorded.digest));
    // The first App check is always nonpassing and exactly one POST can be
    // claimed, even with concurrent clients or a restart after a lost reply.
    let posts_before_pending = posts.load(Ordering::SeqCst);
    let pending_db = data.path().join("pending-check.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [pending_db.to_str().unwrap()])
        .unwrap();
    let mut pending_a = sluss::landing::LandingJournal::open(&pending_db).unwrap();
    assert!(matches!(
        observer_app
            .start_staged_check(
                &mut pending_a,
                (&fixture_token, &security_token, &fixture_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Scope)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), posts_before_pending);
    let mut pending_b = sluss::landing::LandingJournal::open(&pending_db).unwrap();
    let done_a = std::sync::atomic::AtomicBool::new(false);
    let done_b = std::sync::atomic::AtomicBool::new(false);
    mode.store(125, Ordering::SeqCst);
    let (one, two, _) = tokio::join!(
        async {
            let result = observer_app
                .start_staged_check(
                    &mut pending_a,
                    (&fixture_token, &security_token, &check_token),
                    &observer_policy,
                    &selected_trust,
                    id,
                )
                .await;
            done_a.store(true, Ordering::SeqCst);
            result
        },
        async {
            let result = observer_app
                .start_staged_check(
                    &mut pending_b,
                    (&fixture_token, &security_token, &check_token),
                    &observer_policy,
                    &selected_trust,
                    id,
                )
                .await;
            done_b.store(true, Ordering::SeqCst);
            result
        },
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while posts.load(Ordering::SeqCst) == posts_before_pending
                    || !(done_a.load(Ordering::SeqCst) || done_b.load(Ordering::SeqCst))
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            mode.store(73, Ordering::SeqCst);
        }
    );
    assert_eq!(
        [&one, &two]
            .iter()
            .filter(|r| matches!(r, Ok(sluss::github::PendingCheckOutcome::Created(555))))
            .count(),
        1,
        "{one:?} {two:?}"
    );
    assert!(
        [&one, &two]
            .iter()
            .any(|r| matches!(r, Err(sluss::github::PendingCheckError::Uncertain))),
        "{one:?} {two:?}"
    );
    assert_eq!(posts.load(Ordering::SeqCst), posts_before_pending + 1);
    drop(pending_a);
    drop(pending_b);
    let mut pending = sluss::landing::LandingJournal::open(&pending_db).unwrap();
    assert!(matches!(
        pending.pending_ci_check(id).unwrap().unwrap().1,
        sluss::landing::PendingCiCheckState::Created {
            check_id: 555,
            direct: true,
            recovered: false,
            ..
        }
    ));
    assert!(matches!(
        observer_app
            .start_staged_check(
                &mut pending,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Ok(sluss::github::PendingCheckOutcome::AlreadyCreated(555))
    ));
    mode.store(131, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .recover_staged_check(&mut pending, &fixture_token, ("owner", "repo"), id, 555)
            .await,
        Err(sluss::github::PendingCheckError::CandidateMismatch)
    ));
    mode.store(130, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .recover_staged_check(&mut pending, &fixture_token, ("owner", "repo"), id, 555)
            .await,
        Err(sluss::github::PendingCheckError::CandidateMismatch)
    ));
    mode.store(73, Ordering::SeqCst);
    let pending_report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&pending_db)
        .args(["staged-check", "--batch-id", id])
        .output()
        .unwrap();
    assert!(pending_report.status.success());
    assert!(String::from_utf8_lossy(&pending_report.stdout).contains("no CI attestation"));
    let pristine_pending = pending_check_stub().lock().unwrap().clone().unwrap();
    let before_current_cancel = posts.load(Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut pending,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Journal(
            sluss::landing::LandingError::PendingCheckNotStale
        ))
    ));
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut pending,
                (&fixture_token, &fixture_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Scope)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_current_cancel);
    let pending_conn = rusqlite::Connection::open(&pending_db).unwrap();
    // The opt-in worker ignores current evidence and foreign repository scope,
    // then recovers an ambiguous cancellation without sending another PATCH.
    let worker_db = data.path().join("pending-cancel-worker.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [worker_db.to_str().unwrap()])
        .unwrap();
    let before_worker = posts.load(Ordering::SeqCst);
    sluss::worker::cancel_stale_checks_once(&observer_app, &worker_db, &observer_policy)
        .await
        .unwrap();
    let mut foreign_policy = observer_policy.clone();
    foreign_policy.repository = 8;
    assert!(
        sluss::worker::cancel_stale_checks_once(&observer_app, &worker_db, &foreign_policy)
            .await
            .is_err()
    );
    assert_eq!(posts.load(Ordering::SeqCst), before_worker);
    let mut worker_inbox = Inbox::open(&worker_db).unwrap();
    worker_inbox
        .record("worker-review", "pull_request_review", &[80; 32], 42, 7)
        .unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(135, Ordering::SeqCst);
    assert!(
        sluss::worker::cancel_stale_checks_once(&observer_app, &worker_db, &observer_policy)
            .await
            .is_err()
    );
    assert_eq!(posts.load(Ordering::SeqCst), before_worker + 1);
    let worker_journal = sluss::landing::LandingJournal::open(&worker_db).unwrap();
    assert!(matches!(
        worker_journal.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Sending { .. })
    ));
    mode.store(73, Ordering::SeqCst);
    sluss::worker::cancel_stale_checks_once(&observer_app, &worker_db, &observer_policy)
        .await
        .unwrap();
    sluss::worker::cancel_stale_checks_once(&observer_app, &worker_db, &observer_policy)
        .await
        .unwrap();
    assert!(matches!(
        worker_journal.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Cancelled {
            direct: false,
            recovered: true,
            ..
        })
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_worker + 1);
    // The supervisor starts work immediately and remains idempotent across restart.
    let supervised_db = data.path().join("pending-cancel-supervised.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [supervised_db.to_str().unwrap()])
        .unwrap();
    let mut supervised_inbox = Inbox::open(&supervised_db).unwrap();
    supervised_inbox
        .record("supervised-review", "pull_request_review", &[81; 32], 42, 7)
        .unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    let before_supervised = posts.load(Ordering::SeqCst);
    let supervised_app =
        Arc::new(App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap());
    let worker = tokio::spawn(sluss::worker::supervise_stale_check_cancellation(
        supervised_app,
        supervised_db.clone(),
        observer_policy.clone(),
        std::time::Duration::from_millis(10),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let journal = sluss::landing::LandingJournal::open(&supervised_db).unwrap();
            if matches!(
                journal.pending_ci_cancellation(id).unwrap(),
                Some(sluss::landing::PendingCiCancellationState::Cancelled { direct: true, .. })
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    sluss::worker::cancel_stale_checks_once(&observer_app, &supervised_db, &observer_policy)
        .await
        .unwrap();
    assert_eq!(posts.load(Ordering::SeqCst), before_supervised + 1);
    let help = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["serve", "--help"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&help.stdout).contains("--cancel-stale-checks"));
    let missing_config = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["serve", "--cancel-stale-checks"])
        .output()
        .unwrap();
    assert!(!missing_config.status.success());
    assert!(String::from_utf8_lossy(&missing_config.stderr).contains("--config"));
    let cancel_db = data.path().join("pending-cancel-cli.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [cancel_db.to_str().unwrap()])
        .unwrap();
    let mut cancel_inbox = Inbox::open(&cancel_db).unwrap();
    cancel_inbox
        .record("cancel-review", "pull_request_review", &[71; 32], 42, 7)
        .unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    let cancel_db_arg = cancel_db.clone();
    let config_arg = config.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let cancelled = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(cancel_db_arg)
            .args(["cancel-stale-staged-check", "--batch-id", id, "--config"])
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(String::from_utf8_lossy(&cancelled.stdout).contains("Cancelled(555)"));
    let mut cancel_journal = sluss::landing::LandingJournal::open(&cancel_db).unwrap();
    assert!(matches!(
        cancel_journal.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Cancelled {
            check_id: 555,
            direct: true,
            recovered: false,
            ..
        })
    ));
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut cancel_journal,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Ok(sluss::github::PendingCancellationOutcome::AlreadyCancelled(
            555
        ))
    ));
    let cancelled_report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&cancel_db)
        .args(["staged-check", "--batch-id", id])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&cancelled_report.stdout).contains("cancellation: Cancelled"));
    assert_eq!(
        pending_check_stub().lock().unwrap().as_ref().unwrap()["conclusion"],
        "cancelled"
    );
    assert!(
        observer_app
            .start_staged_check(
                &mut cancel_journal,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err()
    );
    // A lost cancellation response is resolved by a read, never a second PATCH.
    let cancel_lost_db = data.path().join("pending-cancel-lost.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [cancel_lost_db.to_str().unwrap()])
        .unwrap();
    let mut cancel_lost_inbox = Inbox::open(&cancel_lost_db).unwrap();
    cancel_lost_inbox
        .record(
            "cancel-lost-review",
            "pull_request_review",
            &[72; 32],
            42,
            7,
        )
        .unwrap();
    let mut cancel_lost = sluss::landing::LandingJournal::open(&cancel_lost_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(135, Ordering::SeqCst);
    let before_cancel_lost = posts.load(Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut cancel_lost,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert!(matches!(
        cancel_lost.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Sending { check_id: 555, .. })
    ));
    drop(cancel_lost);
    let mut cancel_lost = sluss::landing::LandingJournal::open(&cancel_lost_db).unwrap();
    mode.store(73, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut cancel_lost,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_cancel_lost + 1);
    mode.store(131, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .recover_cancelled_staged_check(&mut cancel_lost, &fixture_token, ("owner", "repo"), id)
            .await,
        Err(sluss::github::PendingCheckError::CandidateMismatch)
    ));
    mode.store(130, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .recover_cancelled_staged_check(&mut cancel_lost, &fixture_token, ("owner", "repo"), id)
            .await,
        Err(sluss::github::PendingCheckError::CandidateMismatch)
    ));
    mode.store(73, Ordering::SeqCst);
    let cancel_lost_arg = cancel_lost_db.clone();
    let config_arg = config.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let recovered_cancel = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(cancel_lost_arg)
            .args([
                "recover-cancelled-staged-check",
                "--batch-id",
                id,
                "--config",
            ])
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        recovered_cancel.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered_cancel.stderr)
    );
    assert!(
        String::from_utf8_lossy(&recovered_cancel.stdout).contains("does not prove request origin")
    );
    assert!(matches!(
        cancel_lost.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Cancelled {
            direct: false,
            recovered: true,
            ..
        })
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_cancel_lost + 1);
    // A recovered cancellation may receive the matching direct response later.
    let cancel_late_db = data.path().join("pending-cancel-late.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [cancel_late_db.to_str().unwrap()])
        .unwrap();
    let mut cancel_late_inbox = Inbox::open(&cancel_late_db).unwrap();
    cancel_late_inbox
        .record(
            "cancel-late-review",
            "pull_request_review",
            &[73; 32],
            42,
            7,
        )
        .unwrap();
    let mut cancel_sender = sluss::landing::LandingJournal::open(&cancel_late_db).unwrap();
    let mut cancel_recovery = sluss::landing::LandingJournal::open(&cancel_late_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(136, Ordering::SeqCst);
    let before_cancel_late = posts.load(Ordering::SeqCst);
    let (sent, recovered) = tokio::join!(
        observer_app.cancel_stale_staged_check(
            &mut cancel_sender,
            (&fixture_token, &check_token),
            ("owner", "repo"),
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while posts.load(Ordering::SeqCst) == before_cancel_late {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let outcome = observer_app
                .recover_cancelled_staged_check(
                    &mut cancel_recovery,
                    &fixture_token,
                    ("owner", "repo"),
                    id,
                )
                .await;
            mode.store(73, Ordering::SeqCst);
            outcome
        }
    );
    assert!(matches!(
        sent,
        Ok(sluss::github::PendingCancellationOutcome::Cancelled(555))
    ));
    assert!(matches!(
        recovered,
        Ok(sluss::github::PendingCancellationOutcome::Recovered(555))
    ));
    assert!(matches!(
        cancel_sender.pending_ci_cancellation(id).unwrap(),
        Some(sluss::landing::PendingCiCancellationState::Cancelled {
            direct: true,
            recovered: true,
            ..
        })
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_cancel_late + 1);
    // A newly reverified candidate must prevent an obsolete cancellation.
    let cancel_race_db = data.path().join("pending-cancel-race.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [cancel_race_db.to_str().unwrap()])
        .unwrap();
    let mut cancel_race_inbox = Inbox::open(&cancel_race_db).unwrap();
    cancel_race_inbox
        .record(
            "cancel-race-review",
            "pull_request_review",
            &[74; 32],
            42,
            7,
        )
        .unwrap();
    let event = cancel_race_inbox.pending().unwrap().remove(0);
    let mut cancel_race_store = Store::open(&cancel_race_db).unwrap();
    sluss::reconcile::reconcile_once(
        &observer_app,
        &fixture_token,
        &mut cancel_race_store,
        &event,
        &observer_policy,
    )
    .await
    .unwrap();
    let mut cancel_race = sluss::landing::LandingJournal::open(&cancel_race_db).unwrap();
    let mut refresh_race = sluss::landing::LandingJournal::open(&cancel_race_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(138, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let before_cancel_race = posts.load(Ordering::SeqCst);
    let (cancelled, refreshed) = tokio::join!(
        observer_app.cancel_stale_staged_check(
            &mut cancel_race,
            (&fixture_token, &check_token),
            ("owner", "repo"),
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while reads.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let result = observer_app
                .verify_staged_ci(
                    &mut refresh_race,
                    (&fixture_token, &security_token),
                    &observer_policy,
                    &selected_trust,
                    id,
                )
                .await;
            mode.store(73, Ordering::SeqCst);
            result
        }
    );
    assert!(refreshed.is_ok(), "{refreshed:?}");
    assert!(matches!(
        cancelled,
        Err(sluss::github::PendingCheckError::Journal(
            sluss::landing::LandingError::PendingCheckNotStale
        ))
    ));
    assert!(cancel_race.pending_ci_cancellation(id).unwrap().is_none());
    assert_eq!(posts.load(Ordering::SeqCst), before_cancel_race);
    // Concurrent callers cannot send two cancellation requests.
    let concurrent_db = data.path().join("pending-cancel-concurrent.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [concurrent_db.to_str().unwrap()])
        .unwrap();
    let mut concurrent_inbox = Inbox::open(&concurrent_db).unwrap();
    concurrent_inbox
        .record("concurrent-review", "pull_request_review", &[79; 32], 42, 7)
        .unwrap();
    let mut first = sluss::landing::LandingJournal::open(&concurrent_db).unwrap();
    let mut second = sluss::landing::LandingJournal::open(&concurrent_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(136, Ordering::SeqCst);
    let before_concurrent = posts.load(Ordering::SeqCst);
    let (first_result, second_result) = tokio::join!(
        observer_app.cancel_stale_staged_check(
            &mut first,
            (&fixture_token, &check_token),
            ("owner", "repo"),
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while posts.load(Ordering::SeqCst) == before_concurrent {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let result = observer_app
                .cancel_stale_staged_check(
                    &mut second,
                    (&fixture_token, &check_token),
                    ("owner", "repo"),
                    id,
                )
                .await;
            mode.store(73, Ordering::SeqCst);
            result
        }
    );
    assert!(matches!(
        first_result,
        Ok(sluss::github::PendingCancellationOutcome::Cancelled(555))
    ));
    assert!(matches!(
        second_result,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_concurrent + 1);

    // A fully acknowledged event still invalidates the in-flight observation.
    let event_race_db = data.path().join("pending-cancel-event-race.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [event_race_db.to_str().unwrap()])
        .unwrap();
    let mut event_inbox = Inbox::open(&event_race_db).unwrap();
    event_inbox
        .record("event-race-first", "pull_request_review", &[75; 32], 42, 7)
        .unwrap();
    let event = event_inbox.pending().unwrap().remove(0);
    let mut event_store = Store::open(&event_race_db).unwrap();
    sluss::reconcile::reconcile_once(
        &observer_app,
        &fixture_token,
        &mut event_store,
        &event,
        &observer_policy,
    )
    .await
    .unwrap();
    let mut event_journal = sluss::landing::LandingJournal::open(&event_race_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(138, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let before_event_race = posts.load(Ordering::SeqCst);
    let (result, ()) = tokio::join!(
        observer_app.cancel_stale_staged_check(
            &mut event_journal,
            (&fixture_token, &check_token),
            ("owner", "repo"),
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while reads.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            event_inbox
                .record("event-race-second", "pull_request_review", &[76; 32], 42, 7)
                .unwrap();
            let event = event_inbox.pending().unwrap().remove(0);
            sluss::reconcile::reconcile_once(
                &observer_app,
                &fixture_token,
                &mut event_store,
                &event,
                &observer_policy,
            )
            .await
            .unwrap();
            mode.store(73, Ordering::SeqCst);
        }
    );
    assert!(matches!(
        result,
        Err(sluss::github::PendingCheckError::Journal(
            sluss::landing::LandingError::StaleBatch
        ))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_event_race);
    assert!(event_journal.pending_ci_cancellation(id).unwrap().is_none());

    // A forged success response is uncertain and cannot trigger a retry.
    let forged_db = data.path().join("pending-cancel-forged.sqlite");
    pending_conn
        .execute("VACUUM INTO ?1", [forged_db.to_str().unwrap()])
        .unwrap();
    let mut forged_inbox = Inbox::open(&forged_db).unwrap();
    forged_inbox
        .record("forged-review", "pull_request_review", &[77; 32], 42, 7)
        .unwrap();
    let mut forged = sluss::landing::LandingJournal::open(&forged_db).unwrap();
    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    mode.store(137, Ordering::SeqCst);
    let before_forged = posts.load(Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut forged,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    mode.store(73, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut forged,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert!(matches!(
        observer_app
            .recover_cancelled_staged_check(&mut forged, &fixture_token, ("owner", "repo"), id)
            .await,
        Ok(sluss::github::PendingCancellationOutcome::Recovered(555))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), before_forged + 1);

    *pending_check_stub().lock().unwrap() = Some(pristine_pending.clone());
    let lost_db = data.path().join("pending-check-lost.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [lost_db.to_str().unwrap()])
        .unwrap();
    let mut lost = sluss::landing::LandingJournal::open(&lost_db).unwrap();
    mode.store(126, Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .start_staged_check(
                &mut lost,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert!(matches!(
        lost.pending_ci_check(id).unwrap().unwrap().1,
        sluss::landing::PendingCiCheckState::Sending
    ));
    drop(lost);
    let mut lost = sluss::landing::LandingJournal::open(&lost_db).unwrap();
    mode.store(73, Ordering::SeqCst);
    let after_lost = posts.load(Ordering::SeqCst);
    assert!(matches!(
        observer_app
            .start_staged_check(
                &mut lost,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Uncertain)
    ));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        after_lost,
        "uncertain POST must not be resent"
    );
    assert!(
        observer_app
            .recover_staged_check(&mut lost, &fixture_token, ("owner", "repo"), id, 555)
            .await
            .is_err()
    );
    let lost_arg = lost_db.clone();
    let config_arg = config.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let recover_cli = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(lost_arg)
            .args([
                "recover-staged-check",
                "--batch-id",
                id,
                "--check-id",
                "556",
                "--config",
            ])
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        recover_cli.status.success(),
        "{}",
        String::from_utf8_lossy(&recover_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&recover_cli.stdout).contains("does not prove request origin"));
    assert!(matches!(
        lost.pending_ci_check(id).unwrap().unwrap().1,
        sluss::landing::PendingCiCheckState::Created {
            check_id: 556,
            direct: false,
            recovered: true,
            ..
        }
    ));
    assert!(matches!(
        observer_app
            .start_staged_check(
                &mut lost,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await,
        Ok(sluss::github::PendingCheckOutcome::Recovered(556))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), after_lost);
    // A recovered creation is not proof that our request created the check.
    let mut lost_inbox = Inbox::open(&lost_db).unwrap();
    lost_inbox
        .record("lost-review", "pull_request_review", &[78; 32], 42, 7)
        .unwrap();
    assert!(matches!(
        observer_app
            .cancel_stale_staged_check(
                &mut lost,
                (&fixture_token, &check_token),
                ("owner", "repo"),
                id
            )
            .await,
        Err(sluss::github::PendingCheckError::Journal(
            sluss::landing::LandingError::PendingCheckConflict
        ))
    ));
    assert_eq!(posts.load(Ordering::SeqCst), after_lost);
    sluss::worker::cancel_stale_checks_once(&observer_app, &lost_db, &observer_policy)
        .await
        .unwrap();
    assert_eq!(posts.load(Ordering::SeqCst), after_lost);
    for race_mode in [127, 132, 133] {
        let late_db = data
            .path()
            .join(format!("pending-check-late-{race_mode}.sqlite"));
        candidate_conn
            .execute("VACUUM INTO ?1", [late_db.to_str().unwrap()])
            .unwrap();
        let mut sender = sluss::landing::LandingJournal::open(&late_db).unwrap();
        let mut recovery = sluss::landing::LandingJournal::open(&late_db).unwrap();
        let before_late = posts.load(Ordering::SeqCst);
        mode.store(race_mode, Ordering::SeqCst);
        let (direct, recovered) = tokio::join!(
            observer_app.start_staged_check(
                &mut sender,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            ),
            async {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while posts.load(Ordering::SeqCst) == before_late {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                let recovered = observer_app
                    .recover_staged_check(&mut recovery, &fixture_token, ("owner", "repo"), id, 556)
                    .await;
                mode.store(73, Ordering::SeqCst);
                recovered
            }
        );
        assert!(matches!(
            recovered,
            Ok(sluss::github::PendingCheckOutcome::Recovered(556))
        ));
        drop(sender);
        let mut sender = sluss::landing::LandingJournal::open(&late_db).unwrap();
        if race_mode == 133 {
            assert!(matches!(
                direct,
                Ok(sluss::github::PendingCheckOutcome::Created(556))
            ));
            assert!(matches!(
                sender.pending_ci_check(id).unwrap().unwrap().1,
                sluss::landing::PendingCiCheckState::Created {
                    check_id: 556,
                    direct: true,
                    recovered: true,
                    ..
                }
            ));
            assert!(matches!(
                observer_app
                    .start_staged_check(
                        &mut sender,
                        (&fixture_token, &security_token, &check_token),
                        &observer_policy,
                        &selected_trust,
                        id
                    )
                    .await,
                Ok(sluss::github::PendingCheckOutcome::AlreadyCreated(556))
            ));
        } else {
            assert!(matches!(
                direct,
                Err(sluss::github::PendingCheckError::Uncertain)
            ));
            let expected_other = if race_mode == 127 { 557 } else { 556 };
            assert!(
                matches!(sender.pending_ci_check(id).unwrap().unwrap().1, sluss::landing::PendingCiCheckState::Conflict { check_id:Some(556), other_check_id, .. } if other_check_id == expected_other)
            );
            assert!(
                observer_app
                    .start_staged_check(
                        &mut sender,
                        (&fixture_token, &security_token, &check_token),
                        &observer_policy,
                        &selected_trust,
                        id
                    )
                    .await
                    .is_err()
            );
        }
        assert_eq!(posts.load(Ordering::SeqCst), before_late + 1);
    }
    let stale_send_db = data.path().join("pending-check-stale-send.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [stale_send_db.to_str().unwrap()])
        .unwrap();
    let mut stale_sender = sluss::landing::LandingJournal::open(&stale_send_db).unwrap();
    let mut stale_inbox = Inbox::open(&stale_send_db).unwrap();
    let before_stale_send = posts.load(Ordering::SeqCst);
    mode.store(134, Ordering::SeqCst);
    let (sent, _) = tokio::join!(
        observer_app.start_staged_check(
            &mut stale_sender,
            (&fixture_token, &security_token, &check_token),
            &observer_policy,
            &selected_trust,
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while posts.load(Ordering::SeqCst) == before_stale_send {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            stale_inbox
                .record("check-send-review", "pull_request_review", &[67; 32], 42, 7)
                .unwrap();
            let event = stale_inbox.pending().unwrap().remove(0);
            let mut reconciled = Store::open(&stale_send_db).unwrap();
            sluss::reconcile::reconcile_once(
                &observer_app,
                &fixture_token,
                &mut reconciled,
                &event,
                &observer_policy,
            )
            .await
            .unwrap();
            mode.store(73, Ordering::SeqCst);
        }
    );
    assert!(matches!(
        sent,
        Ok(sluss::github::PendingCheckOutcome::Created(556))
    ));
    assert_eq!(
        stale_sender
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    assert!(matches!(
        stale_sender.pending_ci_check(id).unwrap().unwrap().1,
        sluss::landing::PendingCiCheckState::Created { check_id: 556, .. }
    ));
    assert_eq!(
        pending_check_stub().lock().unwrap().as_ref().unwrap()["status"],
        "in_progress"
    );
    assert_eq!(posts.load(Ordering::SeqCst), before_stale_send + 1);
    let stale_report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&stale_send_db)
        .args(["staged-check", "--batch-id", id])
        .output()
        .unwrap();
    assert!(stale_report.status.success());
    assert!(String::from_utf8_lossy(&stale_report.stdout).contains("no current local evidence"));
    let cli_db = data.path().join("pending-check-cli.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [cli_db.to_str().unwrap()])
        .unwrap();
    let cli_db_arg = cli_db.clone();
    let config_arg = config.clone();
    let policy_arg = trust_file.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let start_cli = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(cli_db_arg)
            .args(["start-staged-check", "--batch-id", id, "--config"])
            .arg(config_arg)
            .arg("--policy")
            .arg(policy_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        start_cli.status.success(),
        "{}",
        String::from_utf8_lossy(&start_cli.stderr)
    );
    assert!(String::from_utf8_lossy(&start_cli.stdout).contains("not an attestation"));
    let legacy_db = data.path().join("pending-check-legacy.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [legacy_db.to_str().unwrap()])
        .unwrap();
    let legacy_conn = rusqlite::Connection::open(&legacy_db).unwrap();
    legacy_conn
        .execute("DROP TABLE failure_check_conflicts", [])
        .unwrap();
    let mut legacy = sluss::landing::LandingJournal::open(&legacy_db).unwrap();
    let before_legacy = posts.load(Ordering::SeqCst);
    assert!(
        observer_app
            .start_staged_check(
                &mut legacy,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err()
    );
    assert!(legacy.pending_ci_check(id).unwrap().is_none());
    assert_eq!(
        posts.load(Ordering::SeqCst),
        before_legacy,
        "legacy check namespace must fail before POST"
    );
    let corrupt_pending_db = data.path().join("pending-check-corrupt.sqlite");
    let pending_conn = rusqlite::Connection::open(&pending_db).unwrap();
    pending_conn
        .execute("VACUUM INTO ?1", [corrupt_pending_db.to_str().unwrap()])
        .unwrap();
    let corrupt_pending_conn = rusqlite::Connection::open(&corrupt_pending_db).unwrap();
    corrupt_pending_conn
        .execute(
            "UPDATE pending_ci_checks_v1 SET body='{}' WHERE batch_id=?1",
            [id],
        )
        .unwrap();
    let corrupt_pending =
        sluss::landing::LandingJournal::open_read_only(&corrupt_pending_db).unwrap();
    assert!(corrupt_pending.pending_ci_check(id).is_err());
    for namespace in ["pending", "failure"] {
        let collision_db = data
            .path()
            .join(format!("pending-namespace-{namespace}.sqlite"));
        candidate_conn
            .execute("VACUUM INTO ?1", [collision_db.to_str().unwrap()])
            .unwrap();
        let mut collision = sluss::landing::LandingJournal::open(&collision_db).unwrap();
        let conn = rusqlite::Connection::open(&collision_db).unwrap();
        if namespace == "pending" {
            conn.execute("INSERT INTO pending_ci_checks_v1(batch_id,body,state,check_id,suite_id,direct,recovered) VALUES('other','{}','created','556','44',1,0)", []).unwrap();
        } else {
            conn.execute("INSERT INTO failure_check_intents(id,body,state,app_id,check_id) VALUES('other','{}','created','17','556')", []).unwrap();
        }
        assert!(matches!(
            observer_app
                .start_staged_check(
                    &mut collision,
                    (&fixture_token, &security_token, &check_token),
                    &observer_policy,
                    &selected_trust,
                    id
                )
                .await,
            Err(sluss::github::PendingCheckError::Uncertain)
        ));
        assert!(matches!(
            collision.pending_ci_check(id).unwrap().unwrap().1,
            sluss::landing::PendingCiCheckState::Conflict {
                check_id: None,
                other_check_id: 556,
                ..
            }
        ));
        drop(collision);
        let collision = sluss::landing::LandingJournal::open(&collision_db).unwrap();
        assert!(matches!(
            collision.pending_ci_check(id).unwrap().unwrap().1,
            sluss::landing::PendingCiCheckState::Conflict {
                check_id: None,
                other_check_id: 556,
                ..
            }
        ));
    }
    // Simulate an event between the persisted provider scan and the send claim.
    let barrier_db = data.path().join("pending-check-revision.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [barrier_db.to_str().unwrap()])
        .unwrap();
    let barrier_conn = rusqlite::Connection::open(&barrier_db).unwrap();
    barrier_conn.execute_batch("CREATE TRIGGER test_advance_revision AFTER UPDATE ON ci_evidence_candidate_heads_v1 BEGIN INSERT INTO webhook_deliveries(delivery_id,event,digest) VALUES('test-intervening','__test__',zeroblob(32)); DELETE FROM webhook_deliveries WHERE delivery_id='test-intervening'; END;").unwrap();
    let mut barrier_journal = sluss::landing::LandingJournal::open(&barrier_db).unwrap();
    let before_barrier = posts.load(Ordering::SeqCst);
    assert!(
        observer_app
            .start_staged_check(
                &mut barrier_journal,
                (&fixture_token, &security_token, &check_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err()
    );
    assert!(barrier_journal.pending_ci_check(id).unwrap().is_none());
    assert_eq!(posts.load(Ordering::SeqCst), before_barrier);
    posts.store(posts_before_pending, Ordering::SeqCst);
    mode.store(73, Ordering::SeqCst);
    let mut completed_scheduler =
        sluss::scheduler::Scheduler::open(&candidate_db, observer_capacity.clone()).unwrap();
    completed_scheduler.complete_ci_only(id).unwrap();
    completed_scheduler
        .release_completed_ci_only(id, 123)
        .unwrap();
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(recorded.clone())
    );
    assert_eq!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .unwrap(),
        recorded
    );
    let mut security_inbox = Inbox::open(&candidate_db).unwrap();
    security_inbox
        .record("security-change", "repository_ruleset", &[51; 32], 42, 7)
        .unwrap();
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    let security_event = security_inbox.pending().unwrap().remove(0);
    let mut security_store = Store::open(&candidate_db).unwrap();
    sluss::reconcile::reconcile_once(
        &observer_app,
        &fixture_token,
        &mut security_store,
        &security_event,
        &observer_policy,
    )
    .await
    .unwrap();
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None,
        "acknowledging a security event must not revive old evidence"
    );
    mode.store(77, Ordering::SeqCst);
    assert!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err()
    );
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    mode.store(73, Ordering::SeqCst);
    assert_eq!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .unwrap(),
        recorded
    );
    assert!(matches!(
        security_inbox
            .record("security-change", "repository_ruleset", &[51; 32], 42, 7)
            .unwrap(),
        sluss::webhook::Record::Duplicate
    ));
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(recorded.clone())
    );
    security_inbox
        .record("other-repository", "repository_ruleset", &[52; 32], 42, 999)
        .unwrap();
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        Some(recorded.clone())
    );
    let migration_db = data.path().join("ci-evidence-migration.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [migration_db.to_str().unwrap()])
        .unwrap();
    let mut migration_store = Store::open(&migration_db).unwrap();
    migration_store.set_paused(true).unwrap();
    let previous_mode = migration_store
        .staging_selection_mode()
        .unwrap()
        .unwrap_or_else(|| "unbound".into());
    assert!(
        migration_store
            .migrate_staging_selection(&previous_mode, &"d".repeat(64), false)
            .unwrap()
    );
    migration_store.set_paused(false).unwrap();
    let mut migration_journal = sluss::landing::LandingJournal::open(&migration_db).unwrap();
    assert_eq!(
        migration_journal
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    assert!(
        observer_app
            .verify_staged_ci(
                &mut migration_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err(),
        "a new selection policy cannot reuse prior evidence"
    );
    let corrupt_db = data.path().join("ci-evidence-corrupt.sqlite");
    candidate_conn
        .execute("VACUUM INTO ?1", [corrupt_db.to_str().unwrap()])
        .unwrap();
    let corrupt_conn = rusqlite::Connection::open(&corrupt_db).unwrap();
    corrupt_conn
        .execute(
            "UPDATE ci_evidence_candidates_v1 SET body='{}' WHERE digest=?1",
            [&recorded.digest],
        )
        .unwrap();
    let mut corrupt_journal = sluss::landing::LandingJournal::open_read_only(&corrupt_db).unwrap();
    assert!(matches!(
        corrupt_journal.inspect_ci_candidate(id, &selected_trust),
        Err(sluss::landing::LandingError::Corrupt)
    ));
    let mut wrong_trust = selected_trust.clone();
    wrong_trust.workflow_id += 1;
    assert!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &wrong_trust,
                id
            )
            .await
            .is_err()
    );
    let mut candidate_inbox = Inbox::open(&candidate_db).unwrap();
    candidate_inbox
        .record("candidate-stale", "pull_request_review", &[33; 32], 42, 7)
        .unwrap();
    assert_eq!(
        candidate_preview
            .inspect_ci_candidate(id, &selected_trust)
            .unwrap(),
        None
    );
    assert!(
        observer_app
            .verify_staged_ci(
                &mut candidate_journal,
                (&fixture_token, &security_token),
                &observer_policy,
                &selected_trust,
                id
            )
            .await
            .is_err()
    );
    mode.store(0, Ordering::SeqCst);
    let output = launch(vec![
        "preview-trusted-batch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--policy".into(),
        trust_file.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("not an attestation or merge authorization")
    );
    let check = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = check
        .query_row("SELECT count(*) FROM landing_intents_v1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "preview must not create a landing intent");
    for invalid in [13, 16, 21, 22, 40, 63, 64, 67, 77, 104, 110, 117] {
        mode.store(invalid, Ordering::SeqCst);
        graphql_counts.store(0, Ordering::SeqCst);
        assert!(
            observer_app
                .preview_trusted_batch(
                    &mut preview,
                    &fixture_token,
                    &security_token,
                    &observer_policy,
                    &selected_trust,
                    id
                )
                .await
                .is_err()
        );
    }
    mode.store(121, Ordering::SeqCst);
    graphql_counts.store(0, Ordering::SeqCst);
    let (raced, _) = tokio::join!(
        observer_app.preview_trusted_batch(
            &mut preview,
            &fixture_token,
            &security_token,
            &observer_policy,
            &selected_trust,
            id
        ),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while graphql_counts.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            inbox
                .record("preview-race", "pull_request_review", &[88; 32], 42, 7)
                .unwrap();
            mode.store(0, Ordering::SeqCst);
        }
    );
    assert!(
        raced.is_err(),
        "a local webhook during provider refresh must invalidate the preview"
    );
    let event = inbox.pending().unwrap().remove(0);
    let mut reconciled = Store::open(&db).unwrap();
    sluss::reconcile::reconcile_once(
        &observer_app,
        &fixture_token,
        &mut reconciled,
        &event,
        &observer_policy,
    )
    .await
    .unwrap();
    Box::pin(exercise_marker_cli(
        &db,
        &data,
        &config,
        &trust_file,
        &key_dir,
        id,
        &mode,
    ))
    .await;
    let unguarded = landing
        .prepare_trusted_batch_with_marker("123456789abcdef0123456789abcdef0", id, &selected_trust)
        .unwrap();
    let marker_token = observer_app.staging_token(42, 7).await.unwrap();
    let posts_before = MARKER_POSTS.load(Ordering::SeqCst);
    assert!(
        observer_app
            .create_landing_marker(
                &mut landing,
                &fixture_token,
                &marker_token,
                &observer_policy,
                &selected_trust,
                "123456789abcdef0123456789abcdef0"
            )
            .await
            .is_err()
    );
    assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), posts_before);
    for invalid in [230, 231, 232, 233, 234, 235, 236, 237] {
        GUARDED_HEAD_READS.store(0, Ordering::SeqCst);
        mode.store(invalid, Ordering::SeqCst);
        assert!(
            Box::pin(observer_app.freeze_landing_heads(
                &mut landing,
                &fixture_token,
                &observer_policy,
                &selected_trust,
                "123456789abcdef0123456789abcdef0"
            ))
            .await
            .is_err(),
            "mode {invalid}"
        );
        assert_eq!(
            landing
                .get("123456789abcdef0123456789abcdef0")
                .unwrap()
                .unwrap()
                .0,
            unguarded
        );
    }
    Box::pin(exercise_head_freeze_race(
        &observer_app,
        &fixture_token,
        &observer_policy,
        &selected_trust,
        &db,
        &data,
        &mode,
    ))
    .await;
    mode.store(0, Ordering::SeqCst);
    let frozen_landing = Box::pin(observer_app.freeze_landing_heads(
        &mut landing,
        &fixture_token,
        &observer_policy,
        &selected_trust,
        "123456789abcdef0123456789abcdef0",
    ))
    .await
    .unwrap();
    assert_eq!(
        frozen_landing.guarded_heads.as_ref().unwrap()[0].reference,
        "refs/heads/feature/19"
    );
    assert_eq!(
        Box::pin(observer_app.freeze_landing_heads(
            &mut landing,
            &fixture_token,
            &observer_policy,
            &selected_trust,
            "123456789abcdef0123456789abcdef0"
        ))
        .await
        .unwrap(),
        frozen_landing
    );
    let request = frozen_landing.graphql_request().unwrap();
    let guard = &request["variables"]["input"]["refUpdates"][3];
    assert_eq!(guard["name"], "refs/heads/feature/19");
    assert_eq!(guard["beforeOid"], "a".repeat(40));
    assert_eq!(guard["afterOid"], "a".repeat(40));
    Box::pin(exercise_marker_creation(
        &observer_app,
        &mut landing,
        &db,
        &data,
        &fixture_token,
        &marker_token,
        &security_token,
        &observer_policy,
        &selected_trust,
        &mode,
    ))
    .await;
    let mut wider_trust = selected_trust.clone();
    wider_trust.runner_ids.insert(2);
    assert!(matches!(
        Box::pin(observer_app.refresh_and_claim_observed_landing(
            &mut landing,
            &fixture_token,
            &security_token,
            &observer_policy,
            &wider_trust,
            "123456789abcdef0123456789abcdef0"
        ))
        .await,
        Err(sluss::github::LandingRefreshError::Journal(
            sluss::landing::LandingError::TrustPolicy
        ))
    ));
    assert_eq!(
        landing
            .get("123456789abcdef0123456789abcdef0")
            .unwrap()
            .unwrap()
            .1,
        sluss::landing::IntentState::Prepared
    );
    for invalid in [
        5, 10, 13, 16, 21, 22, 30, 38, 40, 51, 63, 64, 65, 67, 69, 72, 74, 76, 77, 80, 81, 82, 104,
        105, 106, 108, 109, 110, 111, 112, 113, 114, 115, 117, 119, 120, 230, 231, 232, 233, 234,
        235, 236, 237, 239,
    ] {
        GUARDED_HEAD_READS.store(0, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        graphql_counts.store(0, Ordering::SeqCst);
        mode.store(invalid, Ordering::SeqCst);
        assert!(
            Box::pin(observer_app.refresh_and_claim_observed_landing(
                &mut landing,
                &fixture_token,
                &security_token,
                &observer_policy,
                &selected_trust,
                "123456789abcdef0123456789abcdef0"
            ))
            .await
            .is_err()
        );
        assert_eq!(
            landing
                .get("123456789abcdef0123456789abcdef0")
                .unwrap()
                .unwrap()
                .1,
            sluss::landing::IntentState::Prepared
        );
    }
    for invalid in [201, 202, 203, 204, 205] {
        MARKER_PREFLIGHT_READS.store(0, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        mode.store(invalid, Ordering::SeqCst);
        assert!(
            Box::pin(observer_app.refresh_and_claim_observed_landing(
                &mut landing,
                &fixture_token,
                &security_token,
                &observer_policy,
                &selected_trust,
                "123456789abcdef0123456789abcdef0"
            ))
            .await
            .is_err(),
            "marker mode {invalid}"
        );
        assert_eq!(
            landing
                .get("123456789abcdef0123456789abcdef0")
                .unwrap()
                .unwrap()
                .1,
            sluss::landing::IntentState::Prepared
        );
    }
    mode.store(0, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let mut changed_policy = observer_policy.clone();
    changed_policy.required_reviews = 2;
    assert!(matches!(
        Box::pin(observer_app.refresh_and_claim_observed_landing(
            &mut landing,
            &fixture_token,
            &security_token,
            &changed_policy,
            &selected_trust,
            "123456789abcdef0123456789abcdef0"
        ))
        .await,
        Err(sluss::github::LandingRefreshError::Scope)
    ));
    assert_eq!(
        Box::pin(observer_app.refresh_and_claim_observed_landing(
            &mut landing,
            &fixture_token,
            &security_token,
            &observer_policy,
            &selected_trust,
            "123456789abcdef0123456789abcdef0"
        ))
        .await
        .unwrap(),
        Some(frozen_landing)
    );
    assert_eq!(
        landing
            .get("123456789abcdef0123456789abcdef0")
            .unwrap()
            .unwrap()
            .1,
        sluss::landing::IntentState::Sending
    );
    // A restarted observer reads the same receipt without duplicating measurements.
    sluss::worker::observe_dispatches_once(
        &observer_app,
        &db,
        &observer_capacity,
        &observer_policy,
    )
    .await
    .unwrap();
    let output = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Confirmed { runner_id: 1 }"));
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("provider CI observation: Successful (not an attestation)")
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("0 new completed job measurements"));
    let again = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert!(String::from_utf8_lossy(&again.stdout).contains("0 new completed job measurements"));
    let metrics = launch(vec!["metrics".into()]).await.unwrap();
    assert!(metrics.status.success());
    let metrics_text = String::from_utf8_lossy(&metrics.stdout);
    assert!(metrics_text.contains("observed completed jobs: 1"));
    assert!(metrics_text.contains("observed runner-minutes: 1.00"));
    let output = launch(vec![
        "cancel-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("already claimed"));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let cancel_args = vec![
        "cancel-stale".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ];
    let before_stale = launch(cancel_args.clone()).await.unwrap();
    assert!(!before_stale.status.success());
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    let mut store = Store::open(&db).unwrap();
    store.remove_candidate(19).unwrap();
    mode.store(31, Ordering::SeqCst);
    let worker_app =
        Arc::new(App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap());
    let limits: sluss::scheduler::Capacity =
        toml::from_str(&std::fs::read_to_string(&capacity).unwrap()).unwrap();
    let policy = sluss::config::Config::load(&config).unwrap().policy();
    let worker = tokio::spawn(sluss::worker::supervise_stale_cancellation(
        worker_app,
        db.clone(),
        limits,
        policy,
        std::time::Duration::from_millis(10),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while posts.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    let cancelled = launch(cancel_args.clone()).await.unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(String::from_utf8_lossy(&cancelled.stdout).contains("AlreadyRequested"));
    let repeated = launch(cancel_args.clone()).await.unwrap();
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("AlreadyRequested"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    mode.store(0, Ordering::SeqCst);
    let completed = launch(cancel_args).await.unwrap();
    assert!(completed.status.success());
    assert!(String::from_utf8_lossy(&completed.stdout).contains("Completed"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    let stale_observation = launch(vec![
        "observe-dispatch".into(),
        "--batch-id".into(),
        id.into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
    ])
    .await
    .unwrap();
    assert!(
        stale_observation.status.success(),
        "{}",
        String::from_utf8_lossy(&stale_observation.stderr)
    );
    assert!(
        String::from_utf8_lossy(&stale_observation.stdout)
            .contains("batch is stale; no CI result is accepted")
    );
    mode.store(31, Ordering::SeqCst);
    assert!(
        sluss::worker::release_ci_only_once(&fixture_app, &db, &observer_capacity, &fixture_policy)
            .await
            .is_err()
    );
    mode.store(21, Ordering::SeqCst);
    assert!(
        sluss::worker::release_ci_only_once(&fixture_app, &db, &observer_capacity, &fixture_policy)
            .await
            .is_err()
    );
    mode.store(0, Ordering::SeqCst);
    assert_eq!(
        sluss::worker::release_ci_only_once(&fixture_app, &db, &observer_capacity, &fixture_policy)
            .await
            .unwrap(),
        1
    );
    let mut released = sluss::scheduler::Scheduler::open(&db, observer_capacity.clone()).unwrap();
    assert!(released.pending_dispatches().unwrap().is_empty());
    assert!(matches!(
        store.batch(id).unwrap().unwrap().0.state,
        sluss::batch::State::Invalidated { .. }
    ));
    released.release_invalidated_ci_only(id, 123).unwrap();
    assert!(released.release_invalidated_ci_only(id, 124).is_err());
    assert_eq!(posts.load(Ordering::SeqCst), 2);
    let doctor = launch(vec![
        "doctor".into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--workflow-id".into(),
        "17".into(),
    ])
    .await
    .unwrap();
    assert!(!doctor.status.success());
    let info = String::from_utf8_lossy(&doctor.stdout);
    assert!(info.contains("target: main"));
    assert!(info.contains("active branch rules returned: 2"));
    assert!(info.contains("authenticated GitHub App: sluss-test (ID 17)"));
    assert!(info.contains("active rulesets inspected: 1; bypass lists unavailable: 0"));
    assert!(info.contains("classic branch protection: readable"));
    assert!(info.contains("GraphQL updateRefs and expected-old fields appear in the schema"));
    assert!(info.contains("repository runner inventory matches configured hosts: 1 runners"));
    assert!(info.contains("repository-scoped read, staging and Actions credentials were issued"));
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("NOT READY"));
    assert!(
        String::from_utf8_lossy(&doctor.stderr)
            .contains("reviews, PR metadata and branch policy are not atomically guarded")
    );
    assert!(!info.contains("fake-installation-token"));
    reads.store(0, Ordering::SeqCst);
    mode.store(30, Ordering::SeqCst);
    let unstable_doctor = launch(vec![
        "doctor".into(),
        "--config".into(),
        config.to_str().unwrap().into(),
        "--capacity".into(),
        capacity.to_str().unwrap().into(),
        "--workflow-id".into(),
        "17".into(),
    ])
    .await
    .unwrap();
    assert!(!unstable_doctor.status.success());
    assert!(String::from_utf8_lossy(&unstable_doctor.stderr).contains("UnstableObservation"));
    assert_eq!(posts.load(Ordering::SeqCst), 2);

    let settings = sluss::dispatch::Settings {
        workflow_id: 17,
        required_jobs: ["job-1-0".into()].into(),
        request: sluss::scheduler::Request {
            pools: [("rust".into(), 1)].into(),
        },
        verification_policy: None,
    };
    for (name, fail_after_post) in [
        ("uncertain", true),
        ("successful", false),
        ("base-changed", false),
        ("failed", false),
        ("cancelled-empty", false),
        ("stale-unobserved", false),
    ] {
        mode.store(0, Ordering::SeqCst);
        let auto_db = data.path().join(format!("{name}.sqlite"));
        let mut inbox = Inbox::open(&auto_db).unwrap();
        let mut auto_store = Store::open(&auto_db).unwrap();
        inbox
            .record(&format!("{name}-initial"), "pull_request", &[43; 32], 42, 7)
            .unwrap();
        let event = inbox.pending().unwrap().remove(0);
        sluss::reconcile::reconcile_once(
            &fixture_app,
            &fixture_token,
            &mut auto_store,
            &event,
            &fixture_policy,
        )
        .await
        .unwrap();
        auto_store
            .plan_batch(id, &"b".repeat(40), 1, 1)
            .unwrap()
            .unwrap();
        auto_store
            .bind_policy_paths(id, &[".github/workflows/ci.yml".into()])
            .unwrap();
        auto_store.start_integration(id).unwrap();
        auto_store
            .finish_integration(id, &"a".repeat(40), &"e".repeat(64))
            .unwrap();
        let before = posts.load(Ordering::SeqCst);
        auto_store.set_paused(true).unwrap();
        assert!(
            sluss::worker::dispatch_once(
                &fixture_app,
                &auto_db,
                &fixture_policy,
                &observer_capacity,
                &settings
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(posts.load(Ordering::SeqCst), before);
        auto_store.set_paused(false).unwrap();
        if fail_after_post {
            mode.store(17, Ordering::SeqCst);
            assert!(
                sluss::worker::dispatch_once(
                    &fixture_app,
                    &auto_db,
                    &fixture_policy,
                    &observer_capacity,
                    &settings
                )
                .await
                .is_err()
            );
        } else {
            let (a, b) = tokio::join!(
                sluss::worker::dispatch_once(
                    &fixture_app,
                    &auto_db,
                    &fixture_policy,
                    &observer_capacity,
                    &settings
                ),
                sluss::worker::dispatch_once(
                    &fixture_app,
                    &auto_db,
                    &fixture_policy,
                    &observer_capacity,
                    &settings
                ),
            );
            assert!(
                matches!(a, Ok(Some(sluss::dispatch::Outcome::Sent { .. })))
                    || matches!(b, Ok(Some(sluss::dispatch::Outcome::Sent { .. })))
            );
        }
        assert_eq!(posts.load(Ordering::SeqCst), before + 1);
        mode.store(0, Ordering::SeqCst);
        assert!(
            sluss::worker::dispatch_once(
                &fixture_app,
                &auto_db,
                &fixture_policy,
                &observer_capacity,
                &settings
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(
            posts.load(Ordering::SeqCst),
            before + 1,
            "a restarted worker must not retry an accepted or uncertain POST"
        );
        if name == "cancelled-empty" || name == "stale-unobserved" {
            auto_store.remove_candidate(19).unwrap();
            mode.store(
                if name == "cancelled-empty" { 43 } else { 0 },
                Ordering::SeqCst,
            );
            assert_eq!(
                sluss::worker::release_ci_only_once(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy
                )
                .await
                .unwrap(),
                1
            );
            assert!(matches!(
                auto_store.batch(id).unwrap().unwrap().0.state,
                sluss::batch::State::Invalidated { .. }
            ));
            assert_eq!(posts.load(Ordering::SeqCst), before + 1);
            assert_eq!(
                auto_store.metrics().unwrap().completed_jobs,
                if name == "stale-unobserved" { 1 } else { 0 }
            );
            continue;
        }
        if fail_after_post {
            let key = key_dir.path().join("test-key.pem");
            let path = auto_db.clone();
            let cfg = config.clone();
            let limits_path = capacity.clone();
            let recovered = tokio::task::spawn_blocking(move || {
                Command::new(env!("CARGO_BIN_EXE_sluss"))
                    .arg("--db")
                    .arg(path)
                    .args([
                        "recover-dispatch",
                        "--batch-id",
                        id,
                        "--run-id",
                        "123",
                        "--config",
                    ])
                    .arg(cfg)
                    .arg("--capacity")
                    .arg(limits_path)
                    .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                recovered.status.success(),
                "{}",
                String::from_utf8_lossy(&recovered.stderr)
            );
            assert!(
                String::from_utf8_lossy(&recovered.stdout).contains("receipt source: recovered")
            );
            let scheduler =
                sluss::scheduler::Scheduler::open(&auto_db, observer_capacity.clone()).unwrap();
            assert_eq!(
                scheduler.receipt_source(id).unwrap().as_deref(),
                Some("recovered")
            );
            assert_eq!(posts.load(Ordering::SeqCst), before + 1);
            sluss::worker::observe_dispatches_with_completion(
                &fixture_app,
                &auto_db,
                &observer_capacity,
                &fixture_policy,
                true,
            )
            .await
            .unwrap();
            assert!(matches!(
                auto_store.batch(id).unwrap().unwrap().0.state,
                sluss::batch::State::Testing { .. }
            ));
            assert_eq!(
                sluss::worker::release_ci_only_once(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy
                )
                .await
                .unwrap(),
                0
            );
            assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
            auto_store.remove_candidate(19).unwrap();
            assert_eq!(
                sluss::worker::release_ci_only_once(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy
                )
                .await
                .unwrap(),
                0
            );
            assert_eq!(
                scheduler.pending_dispatches().unwrap().len(),
                1,
                "recovered receipt cannot release a stale reservation"
            );
        } else {
            if name == "failed" {
                mode.store(40, Ordering::SeqCst);
            }
            inbox
                .record("fresh-review", "pull_request_review", &[44; 32], 42, 7)
                .unwrap();
            assert!(
                sluss::worker::observe_dispatches_with_completion(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy,
                    true,
                )
                .await
                .is_err(),
                "pending reconciliation must prevent completion"
            );
            let event = inbox.pending().unwrap().remove(0);
            sluss::reconcile::reconcile_once(
                &fixture_app,
                &fixture_token,
                &mut auto_store,
                &event,
                &fixture_policy,
            )
            .await
            .unwrap();
            sluss::worker::observe_dispatches_with_completion(
                &fixture_app,
                &auto_db,
                &observer_capacity,
                &fixture_policy,
                true,
            )
            .await
            .unwrap();
            let expected = if name == "failed" {
                "failure"
            } else {
                "success"
            };
            assert!(matches!(auto_store.batch(id).unwrap().unwrap().0.state,
                sluss::batch::State::CiComplete { run_id: 123, ref conclusion, .. } if conclusion == expected));
            assert_eq!(
                auto_store.ci_only_completions().unwrap(),
                vec![(19, id.into(), expected.into())]
            );
            assert!(
                auto_store
                    .plan_batch("batch-1123456789abcdef", &"b".repeat(40), 1, 1)
                    .unwrap()
                    .is_none()
            );
            let mut scheduler =
                sluss::scheduler::Scheduler::open(&auto_db, observer_capacity.clone()).unwrap();
            assert_eq!(
                scheduler.pending_dispatches().unwrap().len(),
                1,
                "the reservation is retained before release"
            );
            if name != "failed" {
                mode.store(21, Ordering::SeqCst);
                assert!(
                    sluss::worker::release_ci_only_once(
                        &fixture_app,
                        &auto_db,
                        &observer_capacity,
                        &fixture_policy
                    )
                    .await
                    .is_err()
                );
                assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
                reads.store(0, Ordering::SeqCst);
                mode.store(42, Ordering::SeqCst);
                assert!(
                    sluss::worker::release_ci_only_once(
                        &fixture_app,
                        &auto_db,
                        &observer_capacity,
                        &fixture_policy
                    )
                    .await
                    .is_err()
                );
                assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
                assert!(
                    reads.load(Ordering::SeqCst) >= 7,
                    "rerun must appear after the first provider and inventory reads"
                );
                mode.store(41, Ordering::SeqCst);
                assert!(
                    sluss::worker::release_ci_only_once(
                        &fixture_app,
                        &auto_db,
                        &observer_capacity,
                        &fixture_policy
                    )
                    .await
                    .is_err()
                );
                assert_eq!(scheduler.pending_dispatches().unwrap().len(), 1);
                mode.store(0, Ordering::SeqCst);
            }
            assert_eq!(
                sluss::worker::release_ci_only_once(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy
                )
                .await
                .unwrap(),
                1
            );
            assert!(scheduler.pending_dispatches().unwrap().is_empty());
            let mut changed_capacity = observer_capacity.clone();
            changed_capacity.pools.insert("rust".into(), 2);
            assert!(matches!(
                sluss::scheduler::Scheduler::open(&auto_db, changed_capacity),
                Err(sluss::scheduler::SchedulerError::ConfigBusy)
            ));
            scheduler.release_completed_ci_only(id, 123).unwrap();
            assert!(scheduler.release_completed_ci_only(id, 124).is_err());
            assert_eq!(
                sluss::worker::release_ci_only_once(
                    &fixture_app,
                    &auto_db,
                    &observer_capacity,
                    &fixture_policy
                )
                .await
                .unwrap(),
                0
            );
            let queue = Command::new(env!("CARGO_BIN_EXE_sluss"))
                .arg("--db")
                .arg(&auto_db)
                .arg("queue")
                .output()
                .unwrap();
            assert!(String::from_utf8_lossy(&queue.stdout).contains("not merge approval"));
            if name == "base-changed" {
                let raw = rusqlite::Connection::open(&auto_db).unwrap();
                let digest: String = raw
                    .query_row(
                        "SELECT policy_digest FROM repository_binding WHERE id=1",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                let evidence: String = raw
                    .query_row(
                        "SELECT digest FROM candidate_evidence WHERE number=19",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                let candidate = auto_store.candidates().unwrap().remove(0);
                inbox
                    .record("base-moved", "push", &[45; 32], 42, 7)
                    .unwrap();
                let event = inbox.pending().unwrap().remove(0);
                auto_store
                    .apply_admission_reconciliation_with_base(
                        &event,
                        &digest,
                        &[sluss::store::AdmissionCandidate {
                            candidate,
                            evidence_digest: evidence,
                        }],
                        &"c".repeat(40),
                        "main",
                    )
                    .unwrap();
                assert!(auto_store.ci_only_completions().unwrap().is_empty());
                assert!(
                    auto_store
                        .plan_batch("batch-1123456789abcdef", &"c".repeat(40), 1, 1)
                        .unwrap()
                        .is_some()
                );
            } else {
                let retry = Command::new(env!("CARGO_BIN_EXE_sluss"))
                    .arg("--db")
                    .arg(&auto_db)
                    .args(["retry-ci", "--batch-id", id])
                    .output()
                    .unwrap();
                assert!(retry.status.success());
                assert!(
                    String::from_utf8_lossy(&retry.stdout)
                        .contains("cleared 1 CI-only scheduling holds")
                );
                assert_eq!(auto_store.retry_ci_only(id).unwrap(), 0);
                assert!(
                    auto_store
                        .plan_batch("batch-1123456789abcdef", &"b".repeat(40), 1, 1)
                        .unwrap()
                        .is_some()
                );
            }
            assert!(scheduler.pending_dispatches().unwrap().is_empty());
            assert!(
                scheduler
                    .reserve("next-job", &settings.request)
                    .unwrap()
                    .is_some(),
                "released capacity must be reusable"
            );
        }
    }
    server.abort();
}

async fn recovery_ref(
    State(state): State<ServerState>,
    headers: HeaderMap,
    uri: axum::extract::OriginalUri,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let staging = uri.0.path().contains("/sluss/");
    if mode == 118 && staging {
        state.pulls.fetch_add(1, Ordering::SeqCst);
        while state.mode.load(Ordering::SeqCst) == 118 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    if mode == 117 && staging {
        return (StatusCode::NOT_FOUND, Json(json!({"message":"missing"})));
    }
    let count = if !staging {
        state.pulls.fetch_add(1, Ordering::SeqCst)
    } else {
        0
    };
    let sha = if staging {
        if mode == 112 { "d" } else { "a" }
    } else if mode == 110 || (mode == 114 && count > 0) {
        "a"
    } else if mode == 111 {
        "c"
    } else {
        "b"
    };
    let reference = if mode == 116 && staging {
        "refs/heads/sluss/other"
    } else if staging {
        "refs/heads/sluss/batch-0123456789abcdef"
    } else {
        "refs/heads/main"
    };
    (
        StatusCode::OK,
        Json(
            json!({"ref":reference,"object":{"sha":sha.repeat(if mode == 119 {39} else {40}),"type":if mode == 113 && staging {"tree"} else {"commit"}}}),
        ),
    )
}

#[tokio::test]
async fn uncertain_landing_records_stable_github_refs_without_claiming_success() {
    use sluss::landing::{IntentState, LandingError, LandingJournal, LandingPlan};
    let (key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let server = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/git/ref/heads/main", get(recovery_ref))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(recovery_ref),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads.clone(),
            permissions: writes.clone(),
            token_calls: token_calls.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let app = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let read = app.read_token(42, 7).await.unwrap();
    let inventory = app.runner_inventory_token(42, 7).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("recovery.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    let plan = LandingPlan {
        operation_id: "land-recovery-1".into(),
        batch_id: "batch-0123456789abcdef".into(),
        installation_id: 42,
        repository_id: 7,
        repository_node_id: "R_repo".into(),
        target_ref: "refs/heads/main".into(),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        marker_ref: None,
        guarded_heads: None,
        base_sha: "b".repeat(40),
        staged_sha: "a".repeat(40),
        policy_digest: "c".repeat(64),
        admission_digest: "d".repeat(64),
        ci_evidence_digest: "e".repeat(64),
        trusted_ci_policy_digest: None,
        branch_rules_digest: None,
        branch_security_digest: None,
    };
    journal.prepare(&plan).unwrap();
    assert!(
        app.observe_landing_recovery(&mut journal, &read, ("owner", "repo"), &plan.operation_id)
            .await
            .is_err()
    );
    journal.claim(&plan.operation_id).unwrap().unwrap();
    assert!(
        app.observe_landing_recovery(
            &mut journal,
            &inventory,
            ("owner", "repo"),
            &plan.operation_id
        )
        .await
        .is_err()
    );
    for (state, position, stage) in [
        (0, "base", "a"),
        (110, "staged", "a"),
        (111, "other", "a"),
        (112, "base", "d"),
    ] {
        mode.store(state, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        let observation = app
            .observe_landing_recovery(&mut journal, &read, ("owner", "repo"), &plan.operation_id)
            .await
            .unwrap();
        assert_eq!(observation.target_position(), position);
        assert_eq!(observation.staging_sha, stage.repeat(40));
        assert_eq!(
            journal
                .last_recovery_observation(&plan.operation_id)
                .unwrap(),
            Some(observation)
        );
        assert_eq!(
            journal.get(&plan.operation_id).unwrap().unwrap().1,
            IntentState::Sending
        );
        assert!(journal.claim(&plan.operation_id).unwrap().is_none());
        let mut other = plan.clone();
        other.operation_id = "land-recovery-other".into();
        assert!(matches!(
            journal.prepare(&other),
            Err(LandingError::TargetBusy)
        ));
    }
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count = || {
        conn.query_row(
            "SELECT COUNT(*) FROM landing_recovery_observations_v1",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    };
    assert_eq!(count(), 4);
    for invalid in [63, 113, 114, 116, 117, 119] {
        mode.store(invalid, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        assert!(
            app.observe_landing_recovery(
                &mut journal,
                &read,
                ("owner", "repo"),
                &plan.operation_id
            )
            .await
            .is_err(),
            "mode {invalid}"
        );
        assert_eq!(count(), 4);
    }
    mode.store(110, Ordering::SeqCst);
    let config = dir.path().join("config.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    let db_arg = db.clone();
    let config_arg = config.clone();
    let key_arg = key_dir.path().join("test-key.pem");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args([
                "observe-landing-recovery",
                "--operation-id",
                "land-recovery-1",
                "--config",
            ])
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("target observed at staged"));
    drop(journal);
    let mut journal = LandingJournal::open(&db).unwrap();
    assert_eq!(
        journal
            .last_recovery_observation(&plan.operation_id)
            .unwrap()
            .unwrap()
            .target_position(),
        "staged"
    );
    let report = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .args(["landing-recovery", "--operation-id", "land-recovery-1"])
        .output()
        .unwrap();
    assert!(report.status.success());
    assert!(String::from_utf8_lossy(&report.stdout).contains("does not acknowledge"));
    let direct =
        serde_json::to_vec(&json!({"data":{"updateRefs":{"clientMutationId":plan.operation_id}}}))
            .unwrap();
    journal
        .acknowledge(&plan.operation_id, 200, &direct)
        .unwrap();
    assert_eq!(
        journal.get(&plan.operation_id).unwrap().unwrap().1,
        IntentState::Acknowledged
    );
    assert!(
        app.observe_landing_recovery(&mut journal, &read, ("owner", "repo"), &plan.operation_id)
            .await
            .is_err()
    );
    let mut next = plan.clone();
    next.operation_id = "land-recovery-2".into();
    journal.prepare(&next).unwrap();
    journal.claim(&next.operation_id).unwrap().unwrap();
    mode.store(118, Ordering::SeqCst);
    reads.store(0, Ordering::SeqCst);
    let mut concurrent = LandingJournal::open(&db).unwrap();
    let (observed, acknowledged) = tokio::join!(
        app.observe_landing_recovery(&mut journal, &read, ("owner", "repo"), &next.operation_id),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while reads.load(Ordering::SeqCst) < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let direct = serde_json::to_vec(
                &json!({"data":{"updateRefs":{"clientMutationId":next.operation_id}}}),
            )
            .unwrap();
            let result = concurrent.acknowledge(&next.operation_id, 200, &direct);
            mode.store(0, Ordering::SeqCst);
            result
        }
    );
    assert!(acknowledged.is_ok());
    assert!(observed.is_err());
    assert!(
        journal
            .last_recovery_observation(&next.operation_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    conn.execute(
        "UPDATE landing_recovery_observations_v1 SET digest='bad' WHERE id=?1",
        [&plan.operation_id],
    )
    .unwrap();
    assert!(
        journal
            .last_recovery_observation(&plan.operation_id)
            .is_err()
    );
    server.abort();
}

async fn benchmark_latest(
    State(state): State<ServerState>,
    Path(run): Path<u64>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let mut result = benchmark_run_value(run, if run == 100 { 2 } else { 1 }, mode);
    if mode == 209 && state.permissions.fetch_add(1, Ordering::SeqCst) % 2 == 1 {
        result.0["conclusion"] = json!("failure");
    }
    result
}
async fn benchmark_attempt(
    State(state): State<ServerState>,
    Path((run, attempt)): Path<(u64, u32)>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    benchmark_run_value(run, attempt, state.mode.load(Ordering::SeqCst))
}
fn benchmark_run_value(run: u64, attempt: u32, mode: u8) -> Json<Value> {
    Json(
        json!({"id":run,"run_attempt":attempt,"workflow_id":17,"head_sha":"a".repeat(40),
        "head_branch":"main","path":".github/workflows/ci.yml@refs/heads/main","event":"push",
        "status":if mode == 202 {"in_progress"} else {"completed"},"conclusion":"success",
        "repository":{"id":7},"head_repository":{"id":7}}),
    )
}
async fn benchmark_jobs(
    State(state): State<ServerState>,
    Path((run, attempt)): Path<(u64, u32)>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    assert_eq!(query.get("page").map(String::as_str), Some("1"));
    let mode = state.mode.load(Ordering::SeqCst);
    let day = if run == 100 { "01" } else { "02" };
    let hour = if attempt == 1 { "01" } else { "02" };
    let start = if mode == 205 && run == 100 && attempt == 1 {
        "2025-12-31T23:59:00Z".to_owned()
    } else {
        format!("2026-01-{day}T{hour}:00:00Z")
    };
    let runner: Option<u64> = if mode == 206 && run == 100 && attempt == 1 {
        None
    } else {
        Some(1)
    };
    Json(
        json!({"total_count":1,"jobs":[{"id":run*10+u64::from(attempt),"run_id":run,
        "head_sha":if mode == 208 {"b".repeat(40)} else {"a".repeat(40)},"name":"test",
        "status":"completed","conclusion":"success","started_at":start,
        "completed_at":format!("2026-01-{day}T{hour}:01:00Z"),"runner_id":runner}]}),
    )
}
async fn benchmark_pull(
    State(state): State<ServerState>,
    Path(number): Path<u64>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let read = state.pulls.fetch_add(1, Ordering::SeqCst);
    let day = if number == 19 { "01" } else { "02" };
    Json(
        json!({"number":number,"state":"closed","merged":mode != 203,
        "merged_at":format!("2026-01-{day}T03:{}:00Z", if mode == 207 && read % 2 == 1 {"01"} else {"00"}),
        "base":{"ref":"main","repo":{"id":if mode == 204 {8} else {7}}},"head":if mode == 210 {Value::Null} else {json!({"sha":"a".repeat(40)})}}),
    )
}

#[tokio::test]
async fn benchmark_collection_fetches_attempts_and_merges_without_claiming_completeness() {
    let (key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let run_reads = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route(
            "/repos/owner/repo/actions/runs/{run}",
            get(benchmark_latest),
        )
        .route(
            "/repos/owner/repo/actions/runs/{run}/attempts/{attempt}",
            get(benchmark_attempt),
        )
        .route(
            "/repos/owner/repo/actions/runs/{run}/attempts/{attempt}/jobs",
            get(benchmark_jobs),
        )
        .route("/repos/owner/repo/pulls/{number}", get(benchmark_pull))
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads.clone(),
            permissions: run_reads.clone(),
            token_calls: Arc::new(AtomicUsize::new(0)),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("sluss.toml");
    std::fs::write(&config, format!("version=1\n[github]\napp_client_id='app-client-id'\ninstallation_id=42\nrepository_id=7\nowner='owner'\nrepository='repo'\napi='http://{addr}/'\n[queue]\ntarget='main'\n[checks]\nrequired_reviews=1\n")).unwrap();
    let policy = sluss::config::Config::load(&config).unwrap().policy();
    let request_json = json!({"version":1,"repository_id":7,
        "baseline":{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","run_ids":[100],"pull_numbers":[19]},
        "sluss":{"start":"2026-01-02T00:00:00Z","end":"2026-01-03T00:00:00Z","run_ids":[200],"pull_numbers":[20]}});
    let request: sluss::benchmark::CollectionRequest =
        serde_json::from_value(request_json.clone()).unwrap();
    let app = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = app.read_token(42, 7).await.unwrap();
    let (input, notes) = app
        .collect_benchmark(&token, &policy, &request)
        .await
        .unwrap();
    assert_eq!(input.baseline.jobs.len(), 2);
    assert_eq!(input.baseline.jobs[1].attempt, 2);
    assert_eq!(input.baseline.deliveries.len(), 1);
    assert_eq!(input.sluss.deliveries.len(), 1);
    assert_eq!(notes.jobs_without_timing, 0);
    assert!(!input.baseline.jobs_complete && !input.sluss.deliveries_complete);
    assert!(input.baseline.deliveries[0].ready_at.is_none());
    assert!(input.baseline.deliveries[0].run_id.is_none());
    assert_eq!(input.report().unwrap().runner_time_change_percent, None);
    for bad in [202, 203, 204, 207, 208, 209] {
        mode.store(bad, Ordering::SeqCst);
        reads.store(0, Ordering::SeqCst);
        run_reads.store(0, Ordering::SeqCst);
        assert!(
            app.collect_benchmark(&token, &policy, &request)
                .await
                .is_err(),
            "mode {bad}"
        );
    }
    mode.store(205, Ordering::SeqCst);
    let (partial, notes) = app
        .collect_benchmark(&token, &policy, &request)
        .await
        .unwrap();
    assert_eq!(notes.boundary_crossing_jobs, 1);
    assert_eq!(notes.excluded_jobs[0].job_id, 1001);
    assert_eq!(notes.excluded_jobs[0].reason, "crosses-boundary");
    assert_eq!(partial.baseline.jobs.len(), 1);
    mode.store(206, Ordering::SeqCst);
    let (partial, notes) = app
        .collect_benchmark(&token, &policy, &request)
        .await
        .unwrap();
    assert_eq!(notes.jobs_without_timing, 1);
    assert_eq!(notes.excluded_jobs[0].reason, "missing-runner-or-timing");
    assert_eq!(partial.baseline.jobs.len(), 1);
    mode.store(210, Ordering::SeqCst);
    let (partial, notes) = app
        .collect_benchmark(&token, &policy, &request)
        .await
        .unwrap();
    assert_eq!(partial.sluss.deliveries.len(), 1);
    assert!(notes.merged_heads.is_empty());
    mode.store(0, Ordering::SeqCst);
    let request_path = dir.path().join("selection.json");
    std::fs::write(&request_path, serde_json::to_vec(&request_json).unwrap()).unwrap();
    let db_path = dir.path().join("must-not-exist.sqlite");
    let db_arg = db_path.clone();
    let key_path = key_dir.path().join("test-key.pem");
    let request_arg = request_path.clone();
    let config_arg = config.clone();
    let key_arg = key_path.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args(["collect-benchmark", "--request"])
            .arg(request_arg)
            .arg("--config")
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.baseline.jobs.len(), 2);
    mode.store(205, Ordering::SeqCst);
    let db_arg = db_path.clone();
    let request_arg = request_path.clone();
    let config_arg = config.clone();
    let key_arg = key_path.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db_arg)
            .args(["collect-benchmark", "--request"])
            .arg(request_arg)
            .arg("--config")
            .arg(config_arg)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_arg)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("excluded job 1001 in run 100 attempt 1: crosses-boundary")
    );
    let partial: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(partial.baseline.jobs.len(), 1);
    mode.store(0, Ordering::SeqCst);
    let admission_db = dir.path().join("admission.sqlite");
    let mut admission_inbox = sluss::webhook::Inbox::open(&admission_db).unwrap();
    let mut admission_store = sluss::store::Store::open(&admission_db).unwrap();
    admission_inbox
        .record("observed", "pull_request", &[9; 32], 42, 7)
        .unwrap();
    let event = admission_inbox.pending().unwrap().remove(0);
    admission_store
        .apply_admission_reconciliation_with_base(
            &event,
            &sluss::reconcile::policy_digest(&app, &policy).unwrap(),
            &[sluss::store::AdmissionCandidate {
                candidate: sluss::batch::Candidate {
                    number: 20,
                    head_sha: "a".repeat(40),
                },
                evidence_digest: "d".repeat(64),
            }],
            &"b".repeat(40),
            "main",
        )
        .unwrap();
    let conn = rusqlite::Connection::open(&admission_db).unwrap();
    conn.execute("UPDATE admission_observations_v1 SET first_observed_ms=1767314400000,last_observed_ms=1767315600000", []).unwrap();
    let run_collection = |request_path: std::path::PathBuf,
                          config: std::path::PathBuf,
                          key_path: std::path::PathBuf,
                          admission_db: std::path::PathBuf,
                          db: std::path::PathBuf| {
        Command::new(env!("CARGO_BIN_EXE_sluss"))
            .arg("--db")
            .arg(db)
            .args(["collect-benchmark", "--request"])
            .arg(request_path)
            .arg("--config")
            .arg(config)
            .arg("--admission-db")
            .arg(admission_db)
            .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key_path)
            .output()
            .unwrap()
    };
    let output = tokio::task::spawn_blocking({
        let request_path = request_path.clone();
        let config = config.clone();
        let key_path = key_path.clone();
        let admission_db = admission_db.clone();
        let db_path = db_path.clone();
        move || run_collection(request_path, config, key_path, admission_db, db_path)
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let annotated: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        annotated.sluss.deliveries[0].observed_ready_at.as_deref(),
        Some("2026-01-02T00:40:00Z")
    );
    assert_eq!(annotated.sluss.deliveries[0].ready_at, None);
    assert_eq!(
        annotated.report().unwrap().sluss.observed_median_latency_ms,
        Some(140 * 60_000)
    );
    // Even when the policy is changed back to its old digest, an observation
    // from the earlier epoch must not become a new-policy latency sample.
    let digest = sluss::reconcile::policy_digest(&app, &policy).unwrap();
    admission_store.set_paused(true).unwrap();
    admission_store
        .migrate_admission_policy(&digest, 0, &"e".repeat(64), 42, 7, "main")
        .unwrap();
    admission_store
        .migrate_admission_policy(&"e".repeat(64), 1, &digest, 42, 7, "main")
        .unwrap();
    let output = tokio::task::spawn_blocking({
        let request_path = request_path.clone();
        let config = config.clone();
        let key_path = key_path.clone();
        let admission_db = admission_db.clone();
        let db_path = db_path.clone();
        move || run_collection(request_path, config, key_path, admission_db, db_path)
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let annotated: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(annotated.sluss.deliveries[0].observed_ready_at, None);
    // An observation known to have ended before the merge cannot be matched.
    conn.execute(
        "UPDATE admission_observations_v1 SET ended_ms=1767322800000",
        [],
    )
    .unwrap();
    let output = tokio::task::spawn_blocking({
        let request_path = request_path.clone();
        let config = config.clone();
        let key_path = key_path.clone();
        let admission_db = admission_db.clone();
        let db_path = db_path.clone();
        move || run_collection(request_path, config, key_path, admission_db, db_path)
    })
    .await
    .unwrap();
    assert!(output.status.success());
    let annotated: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(annotated.sluss.deliveries[0].observed_ready_at, None);
    conn.execute("UPDATE admission_observations_v1 SET ended_ms=NULL", [])
        .unwrap();
    conn.execute("UPDATE repository_binding SET repository=8 WHERE id=1", [])
        .unwrap();
    let output = tokio::task::spawn_blocking({
        let request_path = request_path.clone();
        let config = config.clone();
        let key_path = key_path.clone();
        let admission_db = admission_db.clone();
        let db_path = db_path.clone();
        move || run_collection(request_path, config, key_path, admission_db, db_path)
    })
    .await
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    conn.execute("UPDATE repository_binding SET repository=7 WHERE id=1", [])
        .unwrap();
    // A clock anomaly cannot be silently converted into a latency sample.
    conn.execute("UPDATE admission_observations_v1 SET clock_uncertain=1", [])
        .unwrap();
    let output = tokio::task::spawn_blocking({
        let db_path = db_path.clone();
        move || run_collection(request_path, config, key_path, admission_db, db_path)
    })
    .await
    .unwrap();
    assert!(output.status.success());
    let annotated: sluss::benchmark::Input = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(annotated.sluss.deliveries[0].observed_ready_at, None);
    assert!(!db_path.exists());
    server.abort();
}

async fn recovery_marker_ref(
    State(state): State<ServerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let count = state.permissions.fetch_add(1, Ordering::SeqCst);
    if mode == 125 {
        return (StatusCode::NOT_FOUND, Json(json!({"message":"missing"})));
    }
    let sha = if mode == 121 || (mode == 122 && count > 0) {
        "a"
    } else {
        "b"
    };
    let name = if mode == 123 {
        "refs/heads/sluss/receipts/other"
    } else {
        "refs/heads/sluss/receipts/123456789abcdef0123456789abcdef0"
    };
    (
        StatusCode::OK,
        Json(
            json!({"ref":name,"object":{"sha":sha.repeat(40),"type":if mode == 124 {"tree"} else {"commit"}}}),
        ),
    )
}

#[tokio::test]
async fn uncertain_landing_observes_marker_without_resolving_or_unlocking() {
    use sluss::landing::{IntentState, LandingJournal, LandingPlan};
    let (_key_dir, private, public) = keys();
    let mode = Arc::new(AtomicU8::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let marker_reads = Arc::new(AtomicUsize::new(0));
    let token_calls = Arc::new(AtomicUsize::new(0));
    let server = Router::new()
        .route("/app/installations/{id}/access_tokens", post(token))
        .route("/repos/owner/repo", get(dispatch_repo))
        .route("/repos/owner/repo/git/ref/heads/main", get(recovery_ref))
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/batch-0123456789abcdef",
            get(recovery_ref),
        )
        .route(
            "/repos/owner/repo/git/ref/heads/sluss/receipts/123456789abcdef0123456789abcdef0",
            get(recovery_marker_ref),
        )
        .with_state(ServerState {
            public_key: Arc::new(public),
            mode: mode.clone(),
            pulls: reads,
            permissions: marker_reads.clone(),
            token_calls,
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let app = App::new("app-client-id".into(), &private, &format!("http://{addr}/")).unwrap();
    let token = app.read_token(42, 7).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("recovery.sqlite");
    let mut journal = LandingJournal::open(&db).unwrap();
    let id = "123456789abcdef0123456789abcdef0";
    let plan = LandingPlan {
        operation_id: id.into(),
        batch_id: "batch-0123456789abcdef".into(),
        installation_id: 42,
        repository_id: 7,
        repository_node_id: "R_repo".into(),
        target_ref: "refs/heads/main".into(),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        marker_ref: Some(format!("refs/heads/sluss/receipts/{id}")),
        guarded_heads: None,
        base_sha: "b".repeat(40),
        staged_sha: "a".repeat(40),
        policy_digest: "c".repeat(64),
        admission_digest: "d".repeat(64),
        ci_evidence_digest: "e".repeat(64),
        trusted_ci_policy_digest: None,
        branch_rules_digest: None,
        branch_security_digest: None,
    };
    journal.prepare(&plan).unwrap();
    // Model an already-claimed provider write; this test only observes it.
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE landing_intents_v1 SET state='sending' WHERE id=?1",
        [id],
    )
    .unwrap();
    for (current, expected) in [(120, "b"), (121, "a")] {
        mode.store(current, Ordering::SeqCst);
        marker_reads.store(0, Ordering::SeqCst);
        let observation = app
            .observe_landing_recovery(&mut journal, &token, ("owner", "repo"), id)
            .await
            .unwrap();
        assert_eq!(observation.marker_sha, Some(expected.repeat(40)));
        assert_eq!(
            journal.last_recovery_observation(id).unwrap(),
            Some(observation)
        );
        assert_eq!(journal.get(id).unwrap().unwrap().1, IntentState::Sending);
        assert!(matches!(
            journal.claim(id),
            Err(sluss::landing::LandingError::BatchBound)
        ));
    }
    for current in [122, 123, 124, 125] {
        mode.store(current, Ordering::SeqCst);
        marker_reads.store(0, Ordering::SeqCst);
        assert!(
            app.observe_landing_recovery(&mut journal, &token, ("owner", "repo"), id)
                .await
                .is_err(),
            "mode {current}"
        );
        assert_eq!(
            journal
                .last_recovery_observation(id)
                .unwrap()
                .unwrap()
                .marker_sha,
            Some("a".repeat(40))
        );
        assert_eq!(journal.get(id).unwrap().unwrap().1, IntentState::Sending);
    }
    drop(journal);
    let reopened = LandingJournal::open_read_only(&db).unwrap();
    assert_eq!(
        reopened
            .last_recovery_observation(id)
            .unwrap()
            .unwrap()
            .marker_sha,
        Some("a".repeat(40))
    );
    server.abort();
}

static MARKER_PREFLIGHT_READS: AtomicUsize = AtomicUsize::new(0);

async fn landing_marker_preflight(
    State(state): State<ServerState>,
    Path(marker_id): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 217 {
        MARKER_PREFLIGHT_READS.fetch_add(1, Ordering::SeqCst);
        while state.mode.load(Ordering::SeqCst) == 217 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    if matches!(mode, 202 | 211) {
        return (StatusCode::NOT_FOUND, Json(json!({"message":"missing"})));
    }
    if mode == 215 {
        return (StatusCode::FORBIDDEN, Json(json!({"message":"forbidden"})));
    }
    let sha = if mode == 201
        || (matches!(mode, 205 | 212) && MARKER_PREFLIGHT_READS.fetch_add(1, Ordering::SeqCst) > 0)
    {
        "a"
    } else {
        "b"
    };
    let reference = if matches!(mode, 203 | 213) {
        "refs/heads/sluss/receipts/other".into()
    } else {
        format!("refs/heads/sluss/receipts/{marker_id}")
    };
    (
        StatusCode::OK,
        Json(
            json!({"ref":reference,"object":{"sha":sha.repeat(40),"type":if matches!(mode, 204 | 214) {"tree"} else {"commit"}}}),
        ),
    )
}

static MARKER_POSTS: AtomicUsize = AtomicUsize::new(0);

async fn create_landing_marker_stub(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let marker = body["ref"].as_str().unwrap();
    let suffix = marker.strip_prefix("refs/heads/sluss/receipts/").unwrap();
    assert!(
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    );
    assert_eq!(body["sha"], "b".repeat(40));
    MARKER_POSTS.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 209 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if mode == 206 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"message":"uncertain"})),
        );
    }
    let sha = if mode == 207 { "a" } else { "b" };
    let name = if mode == 208 {
        "refs/heads/sluss/receipts/other"
    } else {
        marker
    };
    (
        StatusCode::CREATED,
        Json(json!({"ref":name,"object":{"sha":sha.repeat(40),"type":"commit"}})),
    )
}

#[allow(clippy::too_many_arguments)]
async fn exercise_marker_creation(
    app: &App,
    journal: &mut sluss::landing::LandingJournal,
    db: &std::path::Path,
    data: &tempfile::TempDir,
    read: &sluss::github::InstallationToken,
    write: &sluss::github::InstallationToken,
    security: &sluss::github::InstallationToken,
    repository: &sluss::reconcile::RepositoryPolicy,
    trust: &sluss::landing::TrustedCiPolicy,
    mode: &AtomicU8,
) {
    use sluss::landing::{IntentState, MarkerCreationState};
    let id = "123456789abcdef0123456789abcdef0";
    mode.store(0, Ordering::SeqCst);
    MARKER_POSTS.store(0, Ordering::SeqCst);
    assert_eq!(
        journal.marker_creation_state(id).unwrap(),
        MarkerCreationState::Prepared
    );
    assert!(
        app.refresh_and_claim_observed_landing(journal, read, security, repository, trust, id)
            .await
            .is_err()
    );
    assert_eq!(journal.get(id).unwrap().unwrap().1, IntentState::Prepared);
    assert!(
        app.create_landing_marker(journal, read, read, repository, trust, id)
            .await
            .is_err()
    );
    assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), 0);
    let conn = rusqlite::Connection::open(db).unwrap();
    for invalid in [206, 207, 208] {
        let copy = data.path().join(format!("marker-{invalid}.sqlite"));
        conn.execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
            .unwrap();
        let mut uncertain = sluss::landing::LandingJournal::open(&copy).unwrap();
        mode.store(invalid, Ordering::SeqCst);
        let before = MARKER_POSTS.load(Ordering::SeqCst);
        assert!(
            app.create_landing_marker(&mut uncertain, read, write, repository, trust, id)
                .await
                .is_err()
        );
        assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
        drop(uncertain);
        let mut uncertain = sluss::landing::LandingJournal::open(&copy).unwrap();
        assert_eq!(
            uncertain.marker_creation_state(id).unwrap(),
            MarkerCreationState::Creating
        );
        mode.store(0, Ordering::SeqCst);
        assert!(
            app.create_landing_marker(&mut uncertain, read, write, repository, trust, id)
                .await
                .is_err()
        );
        assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
        assert!(
            app.refresh_and_claim_observed_landing(
                &mut uncertain,
                read,
                security,
                repository,
                trust,
                id
            )
            .await
            .is_err()
        );
        assert_eq!(uncertain.get(id).unwrap().unwrap().1, IntentState::Prepared);
    }
    let concurrent = data.path().join("marker-concurrent.sqlite");
    conn.execute("VACUUM INTO ?1", [concurrent.to_str().unwrap()])
        .unwrap();
    let mut first = sluss::landing::LandingJournal::open(&concurrent).unwrap();
    let mut second = sluss::landing::LandingJournal::open(&concurrent).unwrap();
    mode.store(209, Ordering::SeqCst);
    let before = MARKER_POSTS.load(Ordering::SeqCst);
    let (one, two) = tokio::join!(
        app.create_landing_marker(&mut first, read, write, repository, trust, id),
        app.create_landing_marker(&mut second, read, write, repository, trust, id),
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
    assert_eq!(
        first.marker_creation_state(id).unwrap(),
        MarkerCreationState::Created
    );
    mode.store(0, Ordering::SeqCst);
    let before = MARKER_POSTS.load(Ordering::SeqCst);
    assert_eq!(
        app.create_landing_marker(journal, read, write, repository, trust, id)
            .await
            .unwrap(),
        MarkerCreationState::Created
    );
    assert_eq!(
        journal.marker_creation_state(id).unwrap(),
        MarkerCreationState::Created
    );
    assert_eq!(
        app.create_landing_marker(journal, read, write, repository, trust, id)
            .await
            .unwrap(),
        MarkerCreationState::Created
    );
    assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
}

async fn exercise_marker_cli(
    db: &std::path::Path,
    data: &tempfile::TempDir,
    config: &std::path::Path,
    policy: &std::path::Path,
    key_dir: &tempfile::TempDir,
    batch: &str,
    mode: &AtomicU8,
) {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut previous_id = String::new();
    for fail in [false, true] {
        let copy = data.path().join(format!("marker-cli-{fail}.sqlite"));
        conn.execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
            .unwrap();
        let launch = |args: Vec<String>| {
            let copy = copy.clone();
            let key = key_dir.path().join("test-key.pem");
            tokio::task::spawn_blocking(move || {
                Command::new(env!("CARGO_BIN_EXE_sluss"))
                    .arg("--db")
                    .arg(copy)
                    .args(args)
                    .env("SLUSS_GITHUB_PRIVATE_KEY_PATH", key)
                    .output()
                    .unwrap()
            })
        };
        let prepared = launch(vec![
            "prepare-landing-marker".into(),
            "--batch-id".into(),
            batch.into(),
            "--policy".into(),
            policy.to_str().unwrap().into(),
        ])
        .await
        .unwrap();
        assert!(
            prepared.status.success(),
            "{}",
            String::from_utf8_lossy(&prepared.stderr)
        );
        let text = String::from_utf8(prepared.stdout).unwrap();
        let id = text.split_whitespace().nth(2).unwrap().to_string();
        assert!(
            id.len() == 32
                && id
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        );
        assert_ne!(id, previous_id);
        previous_id = id.clone();
        let listed = launch(vec!["landing-intents".into()]).await.unwrap();
        assert!(listed.status.success() && String::from_utf8_lossy(&listed.stdout).contains(&id));
        mode.store(0, Ordering::SeqCst);
        let frozen = launch(vec![
            "freeze-landing-heads".into(),
            "--operation-id".into(),
            id.clone(),
            "--config".into(),
            config.to_str().unwrap().into(),
            "--policy".into(),
            policy.to_str().unwrap().into(),
        ])
        .await
        .unwrap();
        assert!(
            frozen.status.success(),
            "{}",
            String::from_utf8_lossy(&frozen.stderr)
        );
        assert!(String::from_utf8_lossy(&frozen.stdout).contains("refs/heads/feature/19"));
        let create = || {
            vec![
                "create-landing-marker".into(),
                "--operation-id".into(),
                id.clone(),
                "--config".into(),
                config.to_str().unwrap().into(),
                "--policy".into(),
                policy.to_str().unwrap().into(),
            ]
        };
        mode.store(if fail { 206 } else { 0 }, Ordering::SeqCst);
        let before = MARKER_POSTS.load(Ordering::SeqCst);
        let first = launch(create()).await.unwrap();
        assert_eq!(
            first.status.success(),
            !fail,
            "{}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
        mode.store(0, Ordering::SeqCst);
        let second = launch(create()).await.unwrap();
        assert_eq!(
            second.status.success(),
            !fail,
            "{}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
        let inspected = launch(vec![
            "landing-recovery".into(),
            "--operation-id".into(),
            id.clone(),
        ])
        .await
        .unwrap();
        assert!(inspected.status.success());
        assert!(
            String::from_utf8_lossy(&inspected.stdout).contains(if fail {
                "Creating"
            } else {
                "Created"
            })
        );
        if fail {
            let observe = || {
                vec![
                    "observe-marker-creation".into(),
                    "--operation-id".into(),
                    id.clone(),
                    "--config".into(),
                    config.to_str().unwrap().into(),
                ]
            };
            mode.store(210, Ordering::SeqCst);
            let visible = launch(observe()).await.unwrap();
            assert!(
                visible.status.success(),
                "{}",
                String::from_utf8_lossy(&visible.stderr)
            );
            assert!(String::from_utf8_lossy(&visible.stdout).contains(&"b".repeat(40)));
            mode.store(211, Ordering::SeqCst);
            let missing = launch(observe()).await.unwrap();
            assert!(missing.status.success());
            assert!(String::from_utf8_lossy(&missing.stdout).contains("not visible"));
            for invalid in [212, 213, 214, 215] {
                MARKER_PREFLIGHT_READS.store(0, Ordering::SeqCst);
                mode.store(invalid, Ordering::SeqCst);
                assert!(
                    !launch(observe()).await.unwrap().status.success(),
                    "mode {invalid}"
                );
                let historical = launch(vec![
                    "landing-recovery".into(),
                    "--operation-id".into(),
                    id.clone(),
                ])
                .await
                .unwrap();
                assert!(String::from_utf8_lossy(&historical.stdout).contains("not visible (404)"));
            }
            MARKER_PREFLIGHT_READS.store(0, Ordering::SeqCst);
            mode.store(217, Ordering::SeqCst);
            let reading = launch(observe());
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while MARKER_PREFLIGHT_READS.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let cancelled = launch(vec![
                "cancel-landing".into(),
                "--operation-id".into(),
                id.clone(),
            ])
            .await
            .unwrap();
            mode.store(0, Ordering::SeqCst);
            assert!(cancelled.status.success());
            assert!(
                !reading.await.unwrap().status.success(),
                "observation must detect concurrent cancellation"
            );
            let historical = launch(vec![
                "landing-recovery".into(),
                "--operation-id".into(),
                id.clone(),
            ])
            .await
            .unwrap();
            assert!(String::from_utf8_lossy(&historical.stdout).contains("not visible (404)"));
            let listed = launch(vec!["landing-intents".into()]).await.unwrap();
            assert!(!String::from_utf8_lossy(&listed.stdout).contains(&id));
            assert!(!launch(create()).await.unwrap().status.success());
            assert_eq!(MARKER_POSTS.load(Ordering::SeqCst), before + 1);
            let observed_after_cancel = launch(observe()).await.unwrap();
            assert!(observed_after_cancel.status.success());
            let historical = launch(vec![
                "landing-recovery".into(),
                "--operation-id".into(),
                id.clone(),
            ])
            .await
            .unwrap();
            assert!(String::from_utf8_lossy(&historical.stdout).contains("Creating"));
            let raw = rusqlite::Connection::open(&copy).unwrap();
            raw.execute("UPDATE landing_marker_observations_v1 SET body='{}' WHERE sequence=(SELECT max(sequence) FROM landing_marker_observations_v1)", []).unwrap();
            let corrupted = launch(vec![
                "landing-recovery".into(),
                "--operation-id".into(),
                id.clone(),
            ])
            .await
            .unwrap();
            assert!(
                !corrupted.status.success(),
                "corrupt historical observations must not be reported"
            );
        }
    }
}

static GUARDED_HEAD_READS: AtomicUsize = AtomicUsize::new(0);

async fn guarded_head_ref(
    State(state): State<ServerState>,
    headers: HeaderMap,
    uri: axum::extract::OriginalUri,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer fake-installation-token");
    let mode = state.mode.load(Ordering::SeqCst);
    let synthetic = uri.0.path().contains("/pull/");
    let count = if !synthetic {
        GUARDED_HEAD_READS.fetch_add(1, Ordering::SeqCst)
    } else {
        0
    };
    if mode == 238 && !synthetic {
        while state.mode.load(Ordering::SeqCst) == 238 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    let sha =
        if mode == 233 || (mode == 234 && synthetic) || (mode == 235 && !synthetic && count > 0) {
            "b"
        } else {
            "a"
        };
    let name = if mode == 237 {
        "refs/heads/other"
    } else if synthetic {
        "refs/pull/19/head"
    } else {
        "refs/heads/feature/19"
    };
    Json(
        json!({"ref":name,"object":{"sha":sha.repeat(40),"type":if mode == 236 {"tree"} else {"commit"}}}),
    )
}

#[allow(clippy::too_many_arguments)]
async fn exercise_head_freeze_race(
    app: &App,
    read: &sluss::github::InstallationToken,
    policy: &sluss::reconcile::RepositoryPolicy,
    trust: &sluss::landing::TrustedCiPolicy,
    db: &std::path::Path,
    data: &tempfile::TempDir,
    mode: &AtomicU8,
) {
    let copy = data.path().join("guard-race.sqlite");
    rusqlite::Connection::open(db)
        .unwrap()
        .execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
        .unwrap();
    let mut journal = sluss::landing::LandingJournal::open(&copy).unwrap();
    let mut inbox = sluss::webhook::Inbox::open(&copy).unwrap();
    let id = "123456789abcdef0123456789abcdef0";
    GUARDED_HEAD_READS.store(0, Ordering::SeqCst);
    mode.store(238, Ordering::SeqCst);
    let (result, _) = tokio::join!(
        app.freeze_landing_heads(&mut journal, read, policy, trust, id),
        async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while GUARDED_HEAD_READS.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            inbox
                .record("guard-freeze-race", "pull_request_review", &[87; 32], 42, 7)
                .unwrap();
            mode.store(0, Ordering::SeqCst);
        }
    );
    assert!(result.is_err());
    assert!(journal.get(id).unwrap().unwrap().0.guarded_heads.is_none());
    assert_eq!(
        journal.marker_creation_state(id).unwrap(),
        sluss::landing::MarkerCreationState::Prepared
    );
}

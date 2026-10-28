use sluss::{
    batch::{Candidate, State},
    github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
    scheduler::{Capacity, DispatchPlan, Host, Request, Scheduler},
    store::{AdmissionCandidate, Store, StoreError},
    webhook::Inbox,
};
use std::collections::{BTreeMap, BTreeSet};

fn setup() -> (tempfile::TempDir, Store, Inbox, Scheduler) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.sqlite");
    let mut inbox = Inbox::open(&db).unwrap();
    let mut store = Store::open(&db).unwrap();
    store.bind_ci_bisection(true).unwrap();
    assert!(matches!(
        store.bind_ci_bisection(false),
        Err(StoreError::PolicyMismatch)
    ));
    inbox
        .record("initial", "pull_request", &[1; 32], 1, 2)
        .unwrap();
    let job = inbox.pending().unwrap().remove(0);
    let candidates: Vec<_> = (1..=4)
        .map(|number| AdmissionCandidate {
            candidate: Candidate {
                number,
                head_sha: format!("{number:040x}"),
            },
            evidence_digest: format!("{number:064x}"),
        })
        .collect();
    store
        .apply_admission_reconciliation_with_base(
            &job,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    let pools = BTreeMap::from([("rust".into(), 1)]);
    let scheduler = Scheduler::open(
        &db,
        Capacity {
            pools: pools.clone(),
            hosts: BTreeMap::from([(
                "sluss-host-test".into(),
                Host {
                    slots: 1,
                    runner_ids: BTreeSet::from([7]),
                    pools,
                },
            )]),
        },
    )
    .unwrap();
    (dir, store, inbox, scheduler)
}

fn plan(store: &mut Store, n: u64) -> Option<sluss::batch::Batch> {
    store
        .plan_batch(&format!("batch-{n:016x}"), &"a".repeat(40), 4, 1)
        .unwrap()
}
fn numbers(batch: &sluss::batch::Batch) -> Vec<u64> {
    batch.candidates.iter().map(|c| c.number).collect()
}
fn finish(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &sluss::batch::Batch,
    run_id: u64,
    conclusion: &str,
) {
    finish_selected(store, scheduler, batch, run_id, conclusion, None);
}

fn finish_selected(
    store: &mut Store,
    scheduler: &mut Scheduler,
    batch: &sluss::batch::Batch,
    run_id: u64,
    conclusion: &str,
    suite: Option<&str>,
) {
    let id = &batch.id;
    store.start_integration(id).unwrap();
    store
        .finish_integration(id, &"b".repeat(40), &"c".repeat(64))
        .unwrap();
    if let Some(suite) = suite {
        store
            .record_selection(
                id,
                &sluss::selective::FrozenSelection {
                    base_sha: batch.base_sha.clone(),
                    integration_sha: "b".repeat(40),
                    policy_digest: "d".repeat(64),
                    selection: sluss::selective::Selection::Selected {
                        suites: [suite.into()].into(),
                        components: ["component".into()].into(),
                    },
                },
            )
            .unwrap();
    }
    let plan = DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "b".repeat(40),
        staging_ref: format!("refs/heads/sluss/{id}"),
        policy_digest: "c".repeat(64),
        required_jobs: ["test".into()].into(),
        suites: [suite.unwrap_or("full").into()].into(),
        selection_policy_digest: suite.map(|_| "d".repeat(64)),
    };
    scheduler
        .reserve_batch_dispatch(
            id,
            &Request {
                pools: [("rust".into(), 1)].into(),
            },
            &plan,
        )
        .unwrap()
        .unwrap();
    assert!(scheduler.claim_dispatch(id).unwrap());
    scheduler.record_receipt(id, run_id).unwrap();
    scheduler.observe_dispatch(id, run_id).unwrap();
    let snapshot = ActionsSnapshot {
        run: ActionsRun {
            id: run_id,
            run_attempt: 1,
            workflow_id: 3,
            head_sha: "b".repeat(40),
            head_branch: Some(format!("sluss/{id}")),
            path: ".github/workflows/ci.yml".into(),
            event: "workflow_dispatch".into(),
            status: "completed".into(),
            conclusion: Some(conclusion.into()),
            repository: ActionsRepository { id: 2 },
            head_repository: Some(ActionsRepository { id: 2 }),
        },
        jobs: vec![ActionsJob {
            id: run_id + 100,
            run_id,
            head_sha: "b".repeat(40),
            name: "test".into(),
            status: "completed".into(),
            conclusion: Some(
                if conclusion == "success" {
                    "success"
                } else {
                    "failure"
                }
                .into(),
            ),
            started_at: None,
            completed_at: None,
            runner_id: Some(7),
        }],
    };
    scheduler.record_ci_observation(id, &snapshot).unwrap();
    scheduler.complete_ci_only(id).unwrap();
    scheduler.complete_ci_only(id).unwrap(); // repeated recovery is idempotent
    scheduler.finish_dispatch(id, run_id).unwrap(); // local test assumes the runner is no longer consuming capacity
}

#[test]
fn splits_only_after_an_independent_successful_sibling() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    assert_eq!(numbers(&root), [1, 2, 3, 4]);
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    assert_eq!(numbers(&left), [1, 2]);
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    assert_eq!(
        numbers(&right),
        [3, 4],
        "the other half runs before deeper isolation"
    );
    finish(&mut store, &mut scheduler, &right, 3, "success");
    let first = plan(&mut store, 4).unwrap();
    assert_eq!(numbers(&first), [1]);
    finish(&mut store, &mut scheduler, &first, 4, "success");
    let second = plan(&mut store, 5).unwrap();
    assert_eq!(numbers(&second), [2]);
    finish(&mut store, &mut scheduler, &second, 5, "failure");
    assert!(plan(&mut store, 6).is_none());
    assert!(
        store.failed_candidates().unwrap().is_empty(),
        "CI observations never label a PR as a confirmed culprit"
    );
    assert!(store.active_batches().unwrap().is_empty());
    assert!(matches!(
        store.batch(&second.id).unwrap().unwrap().0.state,
        State::CiComplete { .. }
    ));
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .iter()
            .any(|(id, state)| id == &root.id && state == "investigating")
    );
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .iter()
            .any(|(id, state)| id == &left.id && state == "needs-review")
    );
}

#[test]
fn shared_failures_and_timeouts_stop_without_blaming_individual_prs() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    let right = plan(&mut store, 3).unwrap();
    finish(&mut store, &mut scheduler, &right, 3, "failure");
    assert!(plan(&mut store, 4).is_none());
    assert!(store.failed_candidates().unwrap().is_empty());
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );

    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "timed_out");
    assert!(plan(&mut store, 2).is_none());
    assert!(store.ci_investigations().unwrap().is_empty());
}

#[test]
fn changed_sibling_inputs_prevent_deeper_isolation() {
    let (_dir, mut store, mut inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish(&mut store, &mut scheduler, &left, 2, "failure");
    inbox
        .record("new-head", "pull_request", &[2; 32], 1, 2)
        .unwrap();
    let event = inbox.pending().unwrap().remove(0);
    let mut candidates: Vec<_> = store
        .candidates()
        .unwrap()
        .into_iter()
        .map(|candidate| AdmissionCandidate {
            evidence_digest: format!("{:064x}", candidate.number),
            candidate,
        })
        .collect();
    candidates[0].candidate.head_sha = "e".repeat(40);
    store
        .apply_admission_reconciliation_with_base(
            &event,
            &"f".repeat(64),
            &candidates,
            &"a".repeat(40),
            "main",
        )
        .unwrap();
    let right = plan(&mut store, 3).unwrap();
    assert_eq!(numbers(&right), [3, 4]);
    finish(&mut store, &mut scheduler, &right, 3, "success");
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "stale".into()))
    );
    let new_work = plan(&mut store, 4).unwrap();
    assert_eq!(numbers(&new_work), [1]);
}

#[test]
fn different_suite_sets_are_inconclusive() {
    let (_dir, mut store, _inbox, mut scheduler) = setup();
    let root = plan(&mut store, 1).unwrap();
    finish(&mut store, &mut scheduler, &root, 1, "failure");
    let left = plan(&mut store, 2).unwrap();
    finish_selected(
        &mut store,
        &mut scheduler,
        &left,
        2,
        "failure",
        Some("fast"),
    );
    let right = plan(&mut store, 3).unwrap();
    finish_selected(
        &mut store,
        &mut scheduler,
        &right,
        3,
        "success",
        Some("other"),
    );
    assert!(plan(&mut store, 4).is_none());
    assert!(
        store
            .ci_investigations()
            .unwrap()
            .contains(&(root.id, "inconclusive".into()))
    );
    assert!(store.failed_candidates().unwrap().is_empty());
}

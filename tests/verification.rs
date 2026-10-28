use sluss::verification::{EvidenceError, ExpectedRun, Job, RunObservation, check_consistency};
use std::collections::BTreeSet;
fn expected() -> ExpectedRun {
    ExpectedRun {
        repository_id: 1,
        run_id: 2,
        attempt: 1,
        workflow_id: 3,
        integration_sha: "a".repeat(40),
        policy_digest: "b".repeat(64),
        required_jobs: BTreeSet::from(["linux".into(), "macos".into()]),
    }
}
fn observed() -> RunObservation {
    let e = expected();
    RunObservation {
        repository_id: e.repository_id,
        run_id: e.run_id,
        attempt: e.attempt,
        workflow_id: e.workflow_id,
        integration_sha: e.integration_sha,
        policy_digest: e.policy_digest,
        completed: true,
        successful: true,
        jobs_complete: true,
        jobs: vec![
            Job {
                id: 4,
                name: "linux".into(),
                completed: true,
                successful: true,
            },
            Job {
                id: 5,
                name: "macos".into(),
                completed: true,
                successful: true,
            },
        ],
    }
}
#[test]
fn matching_evidence_is_consistent_but_mismatched_identity_or_policy_is_rejected() {
    let e = expected();
    let original = observed();
    assert_eq!(check_consistency(&e, &original), Ok(()));
    let mut cases = Vec::new();
    let mut x = original.clone();
    x.repository_id += 1;
    cases.push(x);
    let mut x = original.clone();
    x.run_id += 1;
    cases.push(x);
    let mut x = original.clone();
    x.attempt += 1;
    cases.push(x);
    let mut x = original.clone();
    x.workflow_id += 1;
    cases.push(x);
    let mut x = original.clone();
    x.integration_sha = "c".repeat(40);
    cases.push(x);
    let mut x = original;
    x.policy_digest = "c".repeat(64);
    cases.push(x);
    for case in cases {
        assert_eq!(
            check_consistency(&e, &case),
            Err(EvidenceError::IdentityMismatch)
        );
    }
}
#[test]
fn partial_failed_or_ambiguous_results_fail_closed() {
    let e = expected();
    let mut x = observed();
    x.jobs_complete = false;
    assert_eq!(
        check_consistency(&e, &x),
        Err(EvidenceError::IncompleteJobs)
    );
    x = observed();
    x.successful = false;
    assert_eq!(
        check_consistency(&e, &x),
        Err(EvidenceError::RunNotSuccessful)
    );
    x = observed();
    x.jobs.pop();
    assert_eq!(
        check_consistency(&e, &x),
        Err(EvidenceError::RequiredJobNotSuccessful)
    );
    x = observed();
    x.jobs[0].successful = false;
    assert_eq!(
        check_consistency(&e, &x),
        Err(EvidenceError::RequiredJobNotSuccessful)
    );
    x = observed();
    x.jobs[0].completed = false;
    assert_eq!(
        check_consistency(&e, &x),
        Err(EvidenceError::RequiredJobNotSuccessful)
    );
    x = observed();
    x.jobs[1].name = x.jobs[0].name.clone();
    assert_eq!(check_consistency(&e, &x), Err(EvidenceError::AmbiguousJobs));
    x = observed();
    x.jobs[1].id = x.jobs[0].id;
    assert_eq!(check_consistency(&e, &x), Err(EvidenceError::AmbiguousJobs));
    let mut invalid = e.clone();
    invalid.required_jobs.clear();
    assert_eq!(
        check_consistency(&invalid, &observed()),
        Err(EvidenceError::InvalidExpectation)
    );
}

#[test]
fn physical_host_observation_rejects_wrong_runners_and_multiple_jobs() {
    use sluss::{
        github::{ActionsJob, ActionsRepository, ActionsRun, ActionsSnapshot},
        verification::{Placement, PlacementError, check_host_placement},
    };
    let run = ActionsRun {
        id: 2,
        run_attempt: 1,
        workflow_id: 3,
        head_sha: "a".repeat(40),
        head_branch: Some("sluss/test".into()),
        path: ".github/workflows/ci.yml".into(),
        event: "workflow_dispatch".into(),
        status: "in_progress".into(),
        conclusion: None,
        repository: ActionsRepository { id: 1 },
        head_repository: Some(ActionsRepository { id: 1 }),
    };
    let job = ActionsJob {
        id: 4,
        run_id: 2,
        head_sha: "a".repeat(40),
        name: "test".into(),
        status: "queued".into(),
        conclusion: None,
        started_at: None,
        completed_at: None,
        runner_id: None,
    };
    let mut snapshot = ActionsSnapshot {
        run,
        jobs: vec![job],
    };
    let allowed = BTreeSet::from([10, 11]);
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Ok(Placement::Waiting)
    );
    snapshot.jobs[0].runner_id = Some(10);
    snapshot.jobs[0].status = "in_progress".into();
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Ok(Placement::Confirmed { runner_id: 10 })
    );
    snapshot.jobs[0].runner_id = Some(12);
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Err(PlacementError::WrongHost)
    );
    snapshot.jobs[0].runner_id = None;
    snapshot.jobs[0].status = "completed".into();
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Err(PlacementError::MissingRunner)
    );
    snapshot.jobs[0].runner_id = Some(10);
    snapshot.jobs.push(ActionsJob {
        id: 5,
        ..snapshot.jobs[0].clone()
    });
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Err(PlacementError::MultipleJobs)
    );
    snapshot.jobs[1].conclusion = Some("skipped".into());
    assert_eq!(
        check_host_placement(&snapshot, &allowed),
        Ok(Placement::Confirmed { runner_id: 10 })
    );
    assert_eq!(
        check_host_placement(&snapshot, &BTreeSet::new()),
        Err(PlacementError::InvalidHost)
    );
}

#[test]
fn repository_runner_inventory_detects_aliases_missing_and_busy_capacity() {
    use sluss::{
        github::{RepositoryRunner, RunnerLabel},
        scheduler::{Capacity, Host},
        verification::{InventoryError, check_repository_runner_inventory},
    };
    use std::collections::BTreeMap;
    let pools = BTreeMap::from([("rust".into(), 2)]);
    let capacity = Capacity {
        pools: pools.clone(),
        hosts: BTreeMap::from([(
            "sluss-host-box".into(),
            Host {
                slots: 2,
                runner_ids: BTreeSet::from([1, 2]),
                pools,
            },
        )]),
    };
    let runner = |id| RepositoryRunner {
        id,
        name: format!("runner-{id}"),
        status: "online".into(),
        busy: false,
        labels: vec![
            RunnerLabel {
                name: "self-hosted".into(),
            },
            RunnerLabel {
                name: "SLUSS-HOST-BOX".into(),
            },
        ],
    };
    let runners = vec![runner(1), runner(2)];
    assert_eq!(
        check_repository_runner_inventory(&capacity, &runners),
        Ok(())
    );
    let mut busy = runners.clone();
    busy[0].busy = true;
    assert_eq!(
        check_repository_runner_inventory(&capacity, &busy),
        Err(InventoryError::InsufficientCapacity)
    );
    assert_eq!(
        check_repository_runner_inventory(&capacity, &runners[..1]),
        Err(InventoryError::RunnerMismatch)
    );
    let mut extra = runners.clone();
    extra.push(runner(3));
    assert_eq!(
        check_repository_runner_inventory(&capacity, &extra),
        Err(InventoryError::RunnerMismatch)
    );
    let mut duplicate = capacity.clone();
    duplicate.hosts.insert(
        "sluss-host-other".into(),
        Host {
            slots: 1,
            runner_ids: BTreeSet::from([2]),
            pools: BTreeMap::from([("rust".into(), 1)]),
        },
    );
    assert_eq!(
        check_repository_runner_inventory(&duplicate, &runners),
        Err(InventoryError::InvalidConfiguration)
    );
    let mut wrong = runners.clone();
    wrong[0].labels.retain(|label| label.name != "self-hosted");
    assert_eq!(
        check_repository_runner_inventory(&capacity, &wrong),
        Err(InventoryError::RunnerMismatch)
    );
}

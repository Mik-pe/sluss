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

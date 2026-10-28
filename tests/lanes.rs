use sluss::{
    lanes::{Lane, LanePlan, Policy},
    scheduler::{Capacity, DispatchPlan, Host, Request},
};
use std::collections::{BTreeMap, BTreeSet};

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|s| (*s).to_owned()).collect()
}
fn request(pool: &str) -> Request {
    Request {
        pools: BTreeMap::from([(pool.to_owned(), 1)]),
    }
}
fn fixture() -> (Capacity, LanePlan) {
    let hosts = ["rust", "macos"]
        .into_iter()
        .enumerate()
        .map(|(n, pool)| {
            (
                format!("sluss-host-{pool}"),
                Host {
                    slots: 1,
                    runner_ids: BTreeSet::from([n as u64 + 1]),
                    pools: BTreeMap::from([(pool.into(), 1)]),
                },
            )
        })
        .collect();
    let capacity = Capacity {
        pools: BTreeMap::from([("rust".into(), 1), ("macos".into(), 1)]),
        hosts,
    };
    let plan = LanePlan {
        batch: DispatchPlan {
            installation_id: 1,
            repository_id: 2,
            workflow_id: 3,
            integration_sha: "a".repeat(40),
            staging_ref: "refs/heads/sluss/batch-0000000000000001".into(),
            policy_digest: "b".repeat(64),
            required_jobs: set(&["rust-test", "mac-test"]),
            suites: set(&["rust", "macos"]),
            selection_policy_digest: Some("c".repeat(64)),
        },
        lanes: BTreeMap::from([
            (
                "linux".into(),
                Lane {
                    workflow_id: 3,
                    suites: set(&["rust"]),
                    required_jobs: set(&["rust-test"]),
                    request: request("rust"),
                },
            ),
            (
                "mac".into(),
                Lane {
                    workflow_id: 4,
                    suites: set(&["macos"]),
                    required_jobs: set(&["mac-test"]),
                    request: request("macos"),
                },
            ),
        ]),
    };
    (capacity, plan)
}

#[test]
fn covers_selected_suites_and_runs_on_different_hosts() {
    let (capacity, plan) = fixture();
    assert!(plan.validate(&capacity));
    let encoded = serde_json::to_string(&plan).unwrap();
    assert_eq!(serde_json::from_str::<LanePlan>(&encoded).unwrap(), plan);
}

#[test]
fn refuses_missing_duplicate_and_unselected_suites_or_jobs() {
    let (capacity, original) = fixture();
    let mut plan = original.clone();
    plan.lanes.remove("mac");
    assert!(!plan.validate(&capacity));
    let mut plan = original.clone();
    plan.lanes
        .get_mut("mac")
        .unwrap()
        .suites
        .insert("rust".into());
    assert!(!plan.validate(&capacity));
    let mut plan = original.clone();
    plan.lanes
        .get_mut("mac")
        .unwrap()
        .suites
        .insert("other".into());
    assert!(!plan.validate(&capacity));
    let mut plan = original.clone();
    plan.lanes.get_mut("mac").unwrap().required_jobs.clear();
    assert!(!plan.validate(&capacity));
    let mut plan = original;
    plan.lanes
        .get_mut("mac")
        .unwrap()
        .required_jobs
        .insert("other".into());
    assert!(!plan.validate(&capacity));
}

#[test]
fn rejects_requests_that_cannot_fit_a_single_physical_host() {
    let (capacity, mut plan) = fixture();
    plan.lanes
        .get_mut("mac")
        .unwrap()
        .request
        .pools
        .insert("rust".into(), 1);
    assert!(!plan.validate(&capacity));
    plan.lanes.get_mut("mac").unwrap().request = request("missing");
    assert!(!plan.validate(&capacity));
}

#[test]
fn policy_checks_the_full_suite_universe_before_selecting_lanes() {
    let (capacity, plan) = fixture();
    let policy = Policy {
        version: 1,
        lanes: plan.lanes.clone(),
    };
    let mut selected = plan.batch.clone();
    selected.suites = set(&["rust"]);
    selected.required_jobs.clear();
    selected.workflow_id = 0;
    let resolved = policy
        .plan(selected.clone(), &set(&["rust", "macos"]), &capacity)
        .unwrap();
    assert_eq!(
        resolved
            .lanes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["linux"]
    );
    assert_eq!(resolved.batch.required_jobs, set(&["rust-test"]));
    assert!(
        policy
            .plan(selected.clone(), &set(&["rust"]), &capacity)
            .is_err()
    );
    let mut duplicate = policy.clone();
    duplicate
        .lanes
        .get_mut("mac")
        .unwrap()
        .suites
        .insert("rust".into());
    assert!(
        duplicate
            .plan(selected.clone(), &set(&["rust", "macos"]), &capacity)
            .is_err()
    );
    let mut impossible = policy;
    impossible.lanes.get_mut("mac").unwrap().request = request("missing");
    assert!(
        impossible
            .plan(selected, &set(&["rust", "macos"]), &capacity)
            .is_err()
    );
}

#[test]
fn documented_lane_policy_covers_the_example_verification_and_capacity() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let capacity: Capacity =
        toml::from_str(&std::fs::read_to_string(root.join("capacity.toml")).unwrap()).unwrap();
    let verification = sluss::selective::Policy::load(&root.join("verification.toml")).unwrap();
    let policy = Policy::load(&root.join("lane-policy.toml")).unwrap();
    let suites: BTreeSet<_> = verification.full_suites.into_iter().collect();
    let mut template = fixture().1.batch;
    template.suites = suites.clone();
    let plan = policy.plan(template, &suites, &capacity).unwrap();
    assert_eq!(plan.lanes.len(), 2);
}

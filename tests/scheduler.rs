use sluss::scheduler::{Capacity, Host, Request, Scheduler, SchedulerError};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
};

fn pools(entries: &[(&str, u32)]) -> BTreeMap<String, u32> {
    entries.iter().map(|(k, v)| ((*k).into(), *v)).collect()
}
fn capacity() -> Capacity {
    Capacity {
        pools: pools(&[("rust", 2), ("macos", 1), ("android", 1)]),
        hosts: BTreeMap::from([
            (
                "linux".into(),
                Host {
                    slots: 1,
                    pools: pools(&[("rust", 2)]),
                },
            ),
            (
                "mac".into(),
                Host {
                    slots: 1,
                    pools: pools(&[("rust", 1), ("macos", 1), ("android", 1)]),
                },
            ),
        ]),
    }
}
fn request(entries: &[(&str, u32)]) -> Request {
    Request {
        pools: pools(entries),
    }
}

#[test]
fn overlapping_pools_respect_physical_machine_and_global_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    let mut s = Scheduler::open(&db, capacity()).unwrap();
    let android = request(&[("android", 1)]);
    assert_eq!(s.reserve("a", &android).unwrap().unwrap().host, "mac");
    assert!(s.reserve("b", &request(&[("macos", 1)])).unwrap().is_none());
    assert_eq!(
        s.reserve("c", &request(&[("rust", 1)]))
            .unwrap()
            .unwrap()
            .host,
        "linux"
    );
    assert_eq!(s.reserve("a", &android).unwrap().unwrap().host, "mac");
    assert!(matches!(
        s.reserve("a", &request(&[("macos", 1)])),
        Err(SchedulerError::IdConflict)
    ));
    drop(s);
    let mut s = Scheduler::open(&db, capacity()).unwrap();
    assert!(s.reserve("b", &request(&[("macos", 1)])).unwrap().is_none());
    assert!(s.release("a").unwrap());
    assert!(matches!(
        s.reserve("a", &android),
        Err(SchedulerError::IdFinished)
    ));
    assert!(!s.release("a").unwrap());
    assert_eq!(
        s.reserve("b", &request(&[("macos", 1), ("rust", 1)]))
            .unwrap()
            .unwrap()
            .host,
        "mac"
    );
    assert!(s.reserve("d", &request(&[("rust", 1)])).unwrap().is_none());
}

#[test]
fn different_workers_cannot_oversubscribe_the_same_host() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    Scheduler::open(&db, capacity()).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|n| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut scheduler = Scheduler::open(db, capacity()).unwrap();
                barrier.wait();
                scheduler
                    .reserve(&format!("worker-{n}"), &request(&[("macos", 1)]))
                    .unwrap()
                    .is_some()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
}

#[test]
fn config_changes_wait_for_live_reservations_and_invalidate_old_handles() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    let mut old = Scheduler::open(&db, capacity()).unwrap();
    old.reserve("job", &request(&[("rust", 1)]))
        .unwrap()
        .unwrap();
    let mut changed = capacity();
    changed.pools.insert("rust".into(), 1);
    assert!(matches!(
        Scheduler::open(&db, changed.clone()),
        Err(SchedulerError::ConfigBusy)
    ));
    old.release("job").unwrap();
    let mut new = Scheduler::open(&db, changed).unwrap();
    assert!(matches!(
        old.reserve("old", &request(&[("rust", 1)])),
        Err(SchedulerError::ConfigChanged)
    ));
    assert!(
        new.reserve("new", &request(&[("rust", 1)]))
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        new.reserve("impossible", &request(&[("rust", 2)])),
        Err(SchedulerError::Invalid)
    ));
}

fn dispatch_plan() -> sluss::scheduler::DispatchPlan {
    sluss::scheduler::DispatchPlan {
        installation_id: 1,
        repository_id: 2,
        workflow_id: 3,
        integration_sha: "a".repeat(40),
        staging_ref: "refs/heads/sluss/batch-0123456789abcdef".into(),
        policy_digest: "b".repeat(64),
    }
}

#[test]
fn dispatch_intent_survives_crashes_and_cannot_release_capacity_early() {
    use sluss::scheduler::DispatchState;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    let req = request(&[("macos", 1)]);
    let plan = dispatch_plan();
    assert!(
        scheduler
            .reserve_dispatch("job", &req, &plan)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Prepared
    );
    assert!(matches!(
        scheduler.release("job"),
        Err(SchedulerError::DispatchManaged)
    ));
    let mut changed = plan.clone();
    changed.integration_sha = "c".repeat(40);
    assert!(matches!(
        scheduler.reserve_dispatch("job", &req, &changed),
        Err(SchedulerError::IdConflict)
    ));
    assert!(scheduler.claim_dispatch("job").unwrap());
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    assert!(!scheduler.claim_dispatch("job").unwrap());
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Sending
    );
    assert!(scheduler.reserve("other", &req).unwrap().is_none());
    assert!(matches!(
        scheduler.finish_dispatch("job", 50),
        Err(SchedulerError::DispatchTransition)
    ));
    scheduler.observe_dispatch("job", 50).unwrap();
    scheduler.observe_dispatch("job", 50).unwrap();
    assert!(matches!(
        scheduler.observe_dispatch("job", 51),
        Err(SchedulerError::DispatchTransition)
    ));
    drop(scheduler);
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    assert_eq!(
        scheduler.pending_dispatches().unwrap()[0].state,
        DispatchState::Observed { run_id: 50 }
    );
    assert!(matches!(
        scheduler.finish_dispatch("job", 51),
        Err(SchedulerError::DispatchTransition)
    ));
    scheduler.finish_dispatch("job", 50).unwrap();
    scheduler.finish_dispatch("job", 50).unwrap();
    assert!(scheduler.pending_dispatches().unwrap().is_empty());
    assert!(matches!(
        scheduler.reserve_dispatch("job", &req, &plan),
        Err(SchedulerError::IdFinished)
    ));
    assert!(scheduler.reserve("other", &req).unwrap().is_some());
}

#[test]
fn only_one_worker_claims_a_dispatch_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dispatch.sqlite");
    let mut scheduler = Scheduler::open(&db, capacity()).unwrap();
    scheduler
        .reserve_dispatch("job", &request(&[("macos", 1)]), &dispatch_plan())
        .unwrap();
    drop(scheduler);
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut scheduler = Scheduler::open(db, capacity()).unwrap();
                barrier.wait();
                scheduler.claim_dispatch("job").unwrap()
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
}

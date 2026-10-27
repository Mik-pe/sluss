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

use sluss::{
    admission::{AdmissionError, Policy, evaluate},
    github::{
        PullBase, PullHead, PullLabel, PullObservation, PullRepository, PullSnapshot, PullUser,
        ReviewObservation,
    },
};
use std::collections::BTreeSet;

fn user(id: u64, kind: &str) -> PullUser {
    PullUser {
        id,
        login: format!("user-{id}"),
        kind: kind.into(),
    }
}
fn review(id: u64, reviewer: PullUser, state: &str, sha: Option<&str>) -> ReviewObservation {
    ReviewObservation {
        id,
        user: Some(reviewer),
        state: state.into(),
        commit_id: sha.map(str::to_owned),
        submitted_at: Some(format!("2026-01-01T00:00:{id:02}Z")),
    }
}
fn snapshot() -> PullSnapshot {
    PullSnapshot {
        pull: PullObservation {
            number: 5,
            state: "open".into(),
            draft: false,
            user: user(1, "User"),
            head: PullHead {
                sha: "a".repeat(40),
            },
            base: PullBase {
                sha: "b".repeat(40),
                branch: "main".into(),
                repo: PullRepository { id: 7 },
            },
            labels: vec![],
        },
        reviews: vec![],
    }
}
fn policy() -> Policy {
    Policy {
        target_branch: "main".into(),
        required_reviews: 1,
        block_labels: BTreeSet::from(["do-not-merge".into()]),
        authorized_reviewers: BTreeSet::from([1, 2, 3, 4]),
    }
}
#[test]
fn requires_independent_authorized_human_review_for_current_head() {
    let p = policy();
    let mut s = snapshot();
    s.reviews.push(review(
        1,
        user(1, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    s.reviews
        .push(review(2, user(2, "Bot"), "APPROVED", Some(&"a".repeat(40))));
    s.reviews.push(review(
        3,
        user(9, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    s.reviews.push(review(
        4,
        user(3, "User"),
        "APPROVED",
        Some(&"c".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::InsufficientReviews));
    s.reviews.push(review(
        5,
        user(3, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p).unwrap().reviewers, BTreeSet::from([3]));
    let mut p2 = policy();
    p2.required_reviews = 2;
    assert_eq!(evaluate(&s, &p2), Err(AdmissionError::InsufficientReviews));
}
#[test]
fn decisions_are_per_reviewer_and_comments_do_not_cancel_approvals() {
    let p = policy();
    let mut s = snapshot();
    s.reviews.push(review(
        1,
        user(2, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    s.reviews.push(review(
        2,
        user(2, "User"),
        "COMMENTED",
        Some(&"a".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p).unwrap().reviewers, BTreeSet::from([2]));
    s.reviews.push(review(
        3,
        user(3, "User"),
        "CHANGES_REQUESTED",
        Some(&"c".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::ChangesRequested));
    s.reviews.push(review(
        4,
        user(3, "User"),
        "DISMISSED",
        Some(&"c".repeat(40)),
    ));
    assert!(evaluate(&s, &p).is_ok());
    s.reviews.push(review(
        5,
        user(2, "User"),
        "DISMISSED",
        Some(&"a".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::InsufficientReviews));
}
#[test]
fn blocks_drafts_wrong_targets_closed_prs_and_blocking_labels() {
    let p = policy();
    let mut s = snapshot();
    s.pull.labels.push(PullLabel {
        name: "DO-NOT-MERGE".into(),
    });
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::BlockedLabel));
    s.pull.labels.clear();
    s.pull.draft = true;
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::NotReady));
    s.pull.draft = false;
    s.pull.base.branch = "release".into();
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::NotReady));
    s.pull.base.branch = "main".into();
    s.pull.state = "closed".into();
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::NotReady));
}
#[test]
fn rejects_duplicate_unordered_or_unknown_review_records() {
    let p = policy();
    let mut s = snapshot();
    s.reviews.push(review(
        2,
        user(2, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    s.reviews.push(review(
        1,
        user(3, "User"),
        "APPROVED",
        Some(&"a".repeat(40)),
    ));
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::Invalid));
    s.reviews[1].id = 2;
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::Invalid));
    s.reviews.truncate(1);
    s.reviews[0].state = "UNKNOWN".into();
    assert_eq!(evaluate(&s, &p), Err(AdmissionError::Invalid));
}

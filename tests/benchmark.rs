use serde_json::{Value, json};
use sluss::benchmark::{BenchmarkError, Input};
use std::process::Command;

fn input() -> Value {
    json!({
        "version":1,"repository_id":7,
        "baseline":{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","jobs_complete":true,"deliveries_complete":true,
            "jobs":[
                {"job_id":1,"run_id":11,"attempt":1,"runner_id":10,"started_at":"2026-01-01T01:00:00Z","completed_at":"2026-01-01T01:01:00Z"},
                {"job_id":2,"run_id":12,"attempt":2,"runner_id":10,"started_at":"2026-01-01T02:00:00Z","completed_at":"2026-01-01T02:02:00Z"}
            ],
            "deliveries":[
                {"pr":101,"ready_at":"2026-01-01T00:50:00Z","merged_at":"2026-01-01T01:10:00Z","run_id":11,"attempt":1},
                {"pr":102,"ready_at":"2026-01-01T01:30:00Z","merged_at":"2026-01-01T02:10:00Z","run_id":12,"attempt":2}
            ]},
        "sluss":{"start":"2026-01-02T00:00:00Z","end":"2026-01-03T00:00:00Z","jobs_complete":true,"deliveries_complete":true,
            "jobs":[{"job_id":3,"run_id":13,"attempt":1,"runner_id":10,"started_at":"2026-01-02T01:00:00Z","completed_at":"2026-01-02T01:01:00Z"}],
            "deliveries":[
                {"pr":103,"ready_at":"2026-01-02T00:50:00Z","merged_at":"2026-01-02T01:10:00Z","run_id":13,"attempt":1},
                {"pr":104,"ready_at":"2026-01-02T00:40:00Z","merged_at":"2026-01-02T01:10:00Z","run_id":13,"attempt":1}
            ]}
    })
}
fn report(value: Value) -> Result<sluss::benchmark::Report, BenchmarkError> {
    serde_json::from_value::<Input>(value).unwrap().report()
}
#[test]
fn compares_actual_job_intervals_and_counts_reruns_and_batch_deliveries() {
    let result = report(input()).unwrap();
    assert_eq!(result.baseline.runner_ms, 180_000);
    assert_eq!(result.baseline.rerun_attempts, 1);
    assert_eq!(result.sluss.runner_ms, 60_000);
    assert_eq!(result.sluss.delivered, 2);
    assert_eq!(result.sluss.attempts_with_delivery, 1);
    assert_eq!(result.sluss.attributed_deliveries, 2);
    assert_eq!(result.baseline.median_latency_ms, Some(20 * 60_000));
    assert_eq!(result.baseline.p90_latency_ms, Some(40 * 60_000));
    assert!((result.runner_time_change_percent.unwrap() + 66.6666667).abs() < 0.001);
}
#[test]
fn incomplete_empty_and_missing_ready_data_are_reported_honestly() {
    let mut value = input();
    value["sluss"]["jobs_complete"] = json!(false);
    value["sluss"]["deliveries"][0]["ready_at"] = Value::Null;
    let result = report(value).unwrap();
    assert_eq!(result.runner_time_change_percent, None);
    assert!(!result.sluss.complete);
    assert_eq!(result.sluss.latency_samples, 1);
    let mut empty = input();
    empty["baseline"]["deliveries"] = json!([]);
    assert_eq!(report(empty).unwrap().runner_time_change_percent, None);
}

#[test]
fn locally_observed_admission_is_separate_from_exact_ready_time() {
    let mut value = input();
    value["sluss"]["deliveries"][0]["observed_ready_at"] = json!("2026-01-02T00:55:00Z");
    value["sluss"]["deliveries"][1]["ready_at"] = Value::Null;
    let result = report(value.clone()).unwrap();
    assert_eq!(result.sluss.latency_samples, 1);
    assert_eq!(result.sluss.observed_latency_samples, 1);
    assert_eq!(result.sluss.observed_median_latency_ms, Some(15 * 60_000));
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("observed.json");
    std::fs::write(&input_path, serde_json::to_vec(&value).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .args(["benchmark", "--input"])
        .arg(input_path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("locally observed admission-to-merge: median 15.00 minutes")
    );
    value["sluss"]["deliveries"][0]["observed_ready_at"] = json!("2026-01-02T01:11:00Z");
    assert!(report(value).is_err());
}
#[test]
fn rejects_duplicate_cross_cohort_identity_and_invalid_timing_or_associations() {
    let mut cases = Vec::new();
    let mut v = input();
    v["sluss"]["jobs"][0]["job_id"] = json!(1);
    cases.push(v);
    let mut v = input();
    v["sluss"]["jobs"][0]["run_id"] = json!(11);
    cases.push(v);
    let mut v = input();
    v["sluss"]["deliveries"][0]["pr"] = json!(101);
    cases.push(v);
    let mut v = input();
    v["sluss"]["start"] = json!("2026-01-01T12:00:00Z");
    cases.push(v);
    let mut v = input();
    v["sluss"]["jobs"][0]["completed_at"] = json!("2026-01-03T00:00:00Z");
    cases.push(v);
    let mut v = input();
    v["sluss"]["deliveries"][0]["ready_at"] = json!("2026-01-02T01:11:00Z");
    cases.push(v);
    let mut v = input();
    v["sluss"]["deliveries"][0]["attempt"] = json!(2);
    cases.push(v);
    let mut v = input();
    v["sluss"]["deliveries"][0]["merged_at"] = json!("2026-01-02T01:00:30Z");
    cases.push(v);
    let mut v = input();
    v["baseline"]["jobs"][1]["started_at"] = json!("2026-01-01T01:00:30Z");
    cases.push(v);
    for (index, value) in cases.into_iter().enumerate() {
        assert!(report(value).is_err(), "case {index}");
    }
}
#[test]
fn cli_works_without_creating_database_and_labels_unverified_data() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("benchmark.json");
    let db = temp.path().join("unused.sqlite");
    std::fs::write(&file, serde_json::to_vec(&input()).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sluss"))
        .arg("--db")
        .arg(&db)
        .arg("benchmark")
        .arg("--input")
        .arg(file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("-66.7%"));
    assert!(text.contains("not independently verified"));
    assert!(!db.exists());
}

#[test]
fn collection_selection_rejects_overlaps_duplicates_and_unbounded_inputs() {
    let valid = json!({"version":1,"repository_id":7,
        "baseline":{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z","run_ids":[10],"pull_numbers":[101]},
        "sluss":{"start":"2026-01-02T00:00:00Z","end":"2026-01-03T00:00:00Z","run_ids":[20],"pull_numbers":[102]}});
    let validate = |value: Value| {
        serde_json::from_value::<sluss::benchmark::CollectionRequest>(value)
            .unwrap()
            .validate()
    };
    assert!(validate(valid.clone()).is_ok());
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad["version"] = json!(2);
    cases.push(bad);
    let mut bad = valid.clone();
    bad["repository_id"] = json!(0);
    cases.push(bad);
    let mut bad = valid.clone();
    bad["sluss"]["start"] = json!("2026-01-01T23:00:00Z");
    cases.push(bad);
    let mut bad = valid.clone();
    bad["sluss"]["run_ids"] = json!([10]);
    cases.push(bad);
    let mut bad = valid.clone();
    bad["sluss"]["pull_numbers"] = json!([101]);
    cases.push(bad);
    let mut bad = valid.clone();
    bad["baseline"]["run_ids"] = json!([0]);
    cases.push(bad);
    let mut bad = valid;
    bad["baseline"]["run_ids"] = json!((1..=101).collect::<Vec<_>>());
    cases.push(bad);
    for value in cases {
        assert!(validate(value).is_err());
    }
}

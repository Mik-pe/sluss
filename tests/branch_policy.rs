use serde_json::{Value, json};
use sluss::{
    github::{
        BranchCommit, BranchInfo, BranchRulesSnapshot, BranchSecurityObservation,
        ProtectionObservation, RulesetSecurityObservation,
    },
    verification::{BranchPolicyError, BranchPolicyFinding, assess_visible_branch_policy},
};

fn sample() -> BranchSecurityObservation {
    let update = json!({"type":"update","parameters":{"update_allows_fetch_and_merge":false}});
    BranchSecurityObservation {
        branch: BranchInfo {
            name: "main".into(),
            protected: true,
            commit: BranchCommit {
                sha: "a".repeat(40),
            },
        },
        rules: BranchRulesSnapshot {
            repository_id: 7,
            branch: "main".into(),
            rules: vec![
                json!({"type":"update","parameters":{"update_allows_fetch_and_merge":false},"ruleset_id":10,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"}),
            ],
        },
        protection: ProtectionObservation::Present(
            json!({"enforce_admins":{"enabled":true},"allow_force_pushes":{"enabled":false},"allow_deletions":{"enabled":false},"required_linear_history":{"enabled":false},"required_status_checks":null,"required_pull_request_reviews":null,"restrictions":null}),
        ),
        rulesets: vec![RulesetSecurityObservation {
            id: 10,
            bypass_visible: true,
            detail: json!({"id":10,"target":"branch","enforcement":"active","source_type":"Repository","source":"owner/repo","rules":[update],"bypass_actors":[{"actor_type":"Integration","actor_id":17,"bypass_mode":"always"}]}),
        }],
    }
}
fn findings(value: &BranchSecurityObservation) -> Vec<BranchPolicyFinding> {
    assess_visible_branch_policy(value, 17).unwrap()
}
fn protection(value: &mut BranchSecurityObservation) -> &mut Value {
    match &mut value.protection {
        ProtectionObservation::Present(v) => v,
        _ => panic!("fixture"),
    }
}
#[test]
fn matching_visible_settings_are_diagnostics_not_authorization() {
    let security = sample();
    assert!(findings(&security).is_empty());
    // This only shows the named contradictions were not seen; the runtime
    // must still refuse landing without the separate authorization protocol.
}
#[test]
fn other_actors_and_non_direct_app_bypasses_are_reported() {
    let mut value = sample();
    value.rulesets[0].detail["bypass_actors"]
        .as_array_mut()
        .unwrap()
        .push(json!({"actor_type":"Team","actor_id":4,"bypass_mode":"pull_request"}));
    assert!(findings(&value).contains(&BranchPolicyFinding::OtherBypass(10)));
    value.rulesets[0].detail["bypass_actors"][0]["bypass_mode"] = json!("pull_request");
    let result = findings(&value);
    assert!(result.contains(&BranchPolicyFinding::OtherBypass(10)));
    assert!(result.contains(&BranchPolicyFinding::AppBypassMissing(10)));
    assert!(assess_visible_branch_policy(&value, 0).is_err());
}
#[test]
fn reports_absent_update_restriction_and_upstream_update_exception() {
    let mut value = sample();
    value.rules.rules.clear();
    value.rulesets.clear();
    assert!(findings(&value).contains(&BranchPolicyFinding::NoUpdateRestriction));
    let mut value = sample();
    value.rules.rules[0]["parameters"]["update_allows_fetch_and_merge"] = json!(true);
    value.rulesets[0].detail["rules"][0]["parameters"]["update_allows_fetch_and_merge"] =
        json!(true);
    assert!(findings(&value).contains(&BranchPolicyFinding::UpstreamUpdateUnclear(10)));
}
#[test]
fn reports_classic_protection_contradictions_without_guessing_bypass() {
    let mut value = sample();
    let p = protection(&mut value);
    p["enforce_admins"]["enabled"] = json!(false);
    p["allow_force_pushes"]["enabled"] = json!(true);
    p["allow_deletions"]["enabled"] = json!(true);
    p["required_linear_history"]["enabled"] = json!(true);
    p["required_signatures"] = json!({"enabled":true});
    p["lock_branch"] = json!({"enabled":true});
    p["required_pull_request_reviews"] = json!({"required_approving_review_count":1});
    p["required_status_checks"] = json!({"checks":[]});
    p["restrictions"] = json!({"users":[],"apps":[],"teams":[]});
    let result = findings(&value);
    for finding in [
        BranchPolicyFinding::ClassicAdminsExempt,
        BranchPolicyFinding::ClassicForcePush,
        BranchPolicyFinding::ClassicDeletion,
        BranchPolicyFinding::ClassicLinearHistory,
        BranchPolicyFinding::ClassicSignatures,
        BranchPolicyFinding::ClassicLocked,
        BranchPolicyFinding::ClassicPullRequestProtection,
        BranchPolicyFinding::ClassicPushRestrictions,
        BranchPolicyFinding::ClassicRequiredChecks,
    ] {
        assert!(result.contains(&finding), "missing {finding:?}");
    }
}
#[test]
fn hidden_or_inconsistent_security_cannot_be_assessed() {
    let mut value = sample();
    value.rulesets[0].bypass_visible = false;
    value.rulesets[0]
        .detail
        .as_object_mut()
        .unwrap()
        .remove("bypass_actors");
    assert_eq!(
        assess_visible_branch_policy(&value, 17),
        Err(BranchPolicyError::Incomplete)
    );
    let mut value = sample();
    value.rulesets[0].detail["source"] = json!("owner/other");
    assert_eq!(
        assess_visible_branch_policy(&value, 17),
        Err(BranchPolicyError::Incomplete)
    );
}
#[test]
fn linear_rule_without_direct_bypass_is_reported() {
    let mut value = sample();
    value.rules.rules.push(json!({"type":"required_linear_history","ruleset_id":10,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"}));
    value.rulesets[0].detail["rules"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"required_linear_history"}));
    value.rules.rules.push(json!({"type":"required_signatures","ruleset_id":10,"ruleset_source_type":"Repository","ruleset_source":"owner/repo"}));
    value.rulesets[0].detail["rules"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"required_signatures"}));
    value.rulesets[0].detail["bypass_actors"] = json!([]);
    let result = findings(&value);
    assert!(result.contains(&BranchPolicyFinding::LinearRuleMayBlock(10)));
    assert!(result.contains(&BranchPolicyFinding::AppBypassMissing(10)));
    assert!(result.contains(&BranchPolicyFinding::SignedRuleMayBlock(10)));
}

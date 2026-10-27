# Architecture

Sluss is planned as one Rust application with durable local state. SQLite WAL is the initial storage choice; Git commands in isolated workspaces can provide integration semantics without a custom merge engine. The current implementation is an early core, not an operational service.

The intended components are GitHub App ingress and reconciliation, admission and queue management, frozen batch plans, integration building, capacity-aware CI scheduling, evidence verification, and strict landing. A service restart must reconcile external actions and identifiers rather than blindly repeat them.

## Boundaries

Webhook events are hints, not authoritative admission records. Reconcile current heads, reviews, labels, base, and relevant policy with GitHub; deduplicate deliveries durably and handle out-of-order delivery. Debounce PR updates without losing the latest state. Queue snapshots do not imply approval.

Resource scheduling needs explicit budgets for both logical pools and shared physical hosts; multiple labels on the same machine cannot be treated as independent machines. Batch sizes should be measurable and configurable. Bisection rebuilds and retests each sub-batch; a successful sub-batch never inherits another batch's evidence.

Two operation modes have distinct contracts: CI-only does not publish merge authorization from a batch; strict merge is opt-in and has no squash fallback. Strict landing requires a tested provider-specific compare-and-update mechanism and compatible branch rules. A `doctor` check cannot replace a runtime recheck.

## Delivery sequence

1. Complete core state and persistent webhook deduplication; reconcile admission and stale invalidation.
2. Add resource scheduling and CI-only operation, isolated integration creation, trusted workflow evidence and explicit policy.
3. Implement exact-commit strict landing only after the provider semantics and permissions are tested; add recovery, bisection and operational safeguards.
4. Measure actual runner minutes, queue latency, retries and landed PRs in a real pilot. The targets of 30% fewer runner minutes per merged PR and zero incorrectly authorized merges are acceptance goals, not measured results.

Real-world results, native runners, provider permissions, branch rules and Ugla-specific integration are separate acceptance work. No Ugla-specific rules belong in Sluss itself.

# Architecture

Sluss is one Rust application with durable local state. It uses SQLite WAL and Git commands in isolated workspaces. The development preview includes opt-in workers for reconciliation, planning, staging and CI-only dispatch; those workers have only local simulated-provider coverage and are not production-ready.

The intended components are GitHub App ingress and reconciliation, admission and queue management, frozen batch plans, integration building, capacity-aware CI scheduling, evidence verification, and strict landing. A service restart must reconcile external actions and identifiers rather than blindly repeat them.

## Boundaries

Webhook events are hints, not authoritative admission records. Reconcile current heads, reviews, labels, base, and relevant policy with GitHub; deduplicate deliveries durably and handle out-of-order delivery. Debounce PR updates without losing the latest state. Queue snapshots do not imply approval.

Resource scheduling needs explicit budgets for both logical pools and shared physical hosts; multiple labels on the same machine cannot be treated as independent machines. Batch sizes should be measurable and configurable. Bisection rebuilds and retests each sub-batch; a successful sub-batch never inherits another batch's evidence.

An optional lane plan partitions a frozen suite selection into separate workflows and physical-host reservations. Each lane has a durable dispatch identity, its own provider run, and a one-shot send. A CI-only batch completes after all lanes have direct terminal observations and their reservations are safely released. A full frozen lane policy and comparable workflow observations are required for cross-batch bisection; missing timings or investigation costs keep adaptive cost comparisons unresolved. These scheduling records do not constitute trusted attestation.

Two operation modes have distinct contracts: CI-only does not publish merge authorization from a batch; strict merge is opt-in and has no squash fallback. Strict landing requires a tested provider-specific compare-and-update mechanism and compatible branch rules. A `doctor` check cannot replace a runtime recheck.

## Delivery sequence

1. Complete core state and persistent webhook deduplication; reconcile admission and stale invalidation.
2. Add resource scheduling and CI-only operation, isolated integration creation, trusted workflow evidence and explicit policy.
3. Implement exact-commit strict landing only after the provider semantics and permissions are tested; add recovery, bisection and operational safeguards.
4. Measure actual runner minutes, queue latency, retries and landed PRs in a real pilot. The targets of 30% fewer runner minutes per merged PR and zero incorrectly authorized merges are acceptance goals, not measured results.

Real-world results, native runners, provider permissions, branch rules and Ugla-specific integration are separate acceptance work. No Ugla-specific rules belong in Sluss itself.

The `audit-workflows` command lists event declarations from direct workflow files in an explicitly supplied local commit. It reports unknown syntax instead of treating it as safe and helps operators identify PR and push workflows that may start alongside Sluss. Filters, enabled state, workflow semantics, other actors and external CI remain outside its evidence; it does not alter workflows or establish that any particular build is duplicate. Git may fetch missing objects in a partial clone.

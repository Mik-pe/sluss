# CI-only preview: multiple workflows and hosts

This is a development preview for a disposable test repository. The GitHub App and real runners have not been validated end to end. Sluss does not publish a passing `sluss/verified` check or merge PRs; keep the repository's existing merge rules and workflow in place.

## Prepare the installation

1. Install Rust and Git, then build Sluss with `cargo build --release --locked`. Put `target/release/sluss` on your `PATH`, or use its full path in the commands below.
2. Follow [GitHub App setup](github-app-setup.md) for the `ci` profile. Install the App on the test repository and keep its private key outside the checkout. Do not grant it branch bypass.
3. Copy [sluss.toml](../examples/sluss.toml), [capacity.toml](../examples/capacity.toml), [verification.toml](../examples/verification.toml), and [lane-policy.toml](../examples/lane-policy.toml) to an operator-controlled directory. Set the App client ID, installation ID, repository ID, owner, repository, and target branch. Keep `merge.mode = "disabled"`.
4. Configure each physical host with one dedicated registered repository runner and a unique `sluss-host-*` label. Set its actual runner ID, pool budgets, and the global budgets. The automatic release contract requires one runner and one slot per host; global pool budgets must cover the sum of all hosts' declared pool capacities. Do not let other runner processes or build jobs share those machines outside this contract.
5. Install the dispatch workflows on the target and default branches through the repository's normal review process. Adapt [the example](../examples/github-actions/sluss-ci.yml) or the [reusable workflow adapter](../examples/github-actions/reusable-dispatch.yml). Every executable job in a lane must run on the supplied `sluss_host`, and the workflow must validate and implement the complete supplied `sluss_suites` selection. Record the registered numeric workflow ID and actual GitHub job name for each lane. A lane currently permits one executable job; optional skipped jobs are allowed.
6. In the verification policy, describe the complete suite set and dependency relationships. In the lane policy, assign each suite to exactly one lane and declare that lane's workflow ID, required job names, and resource request. A lane's required jobs must exist for every selection that can activate it.
7. Restrict writes to the `sluss/` staging namespace to the intended App and authorized operators. List every workflow and executable policy dependency for `--policy-file`. Sluss freezes this operator-supplied list; it cannot prove the list is complete. Review independent `push`, `create`, `pull_request`, `workflow_run` and external triggers so staging and PR activity do not start unplanned jobs. The staging guard checks only a subset of those triggers; see [integration](integration.md).

Keep the database, App key, webhook secret, and operator configuration separate from the code and machines that run CI. If webhooks are enabled, expose `/webhook` only through a trusted HTTPS proxy and configure the same secret on GitHub and in the service environment. The service also periodically reconciles the repository when notifications are missed.

## Check and run

Set `SLUSS_GITHUB_PRIVATE_KEY_PATH` to the App key and provide `SLUSS_WEBHOOK_SECRET` through the service's secret mechanism. Run the CI-only diagnostic first, using your paths and repeating `--policy-file` for the complete reviewed list:

```sh
sluss doctor --ci-only \
  --config /secure/sluss.toml --capacity /secure/capacity.toml \
  --verification-policy /secure/verification.toml \
  --lane-policy /secure/lane-policy.toml \
  --policy-file .github/workflows/ci-linux.yml \
  --policy-file .github/workflows/ci-macos.yml
```

The diagnostic checks registration, dispatch inputs, scoped credentials and currently idle matching hosts. It samples each lane separately; it does not prove simultaneous availability, workflow semantics, runner isolation, live execution or merge readiness.

After reviewing the diagnostic, start the preview with one database on a filesystem supporting SQLite WAL and full synchronization:

```sh
sluss --db /secure/sluss.sqlite serve \
  --config /secure/sluss.toml --capacity /secure/capacity.toml \
  --verification-policy /secure/verification.toml \
  --auto-plan --auto-stage \
  --policy-file .github/workflows/ci-linux.yml \
  --policy-file .github/workflows/ci-macos.yml \
  --auto-dispatch --lane-policy /secure/lane-policy.toml \
  --observe-dispatches --complete-ci-only --release-ci-only --cancel-stale
```

Add `--bisect-ci-failures` to investigate failures, `--observe-reruns` for partial rerun accounting, or `--observe-deliveries` for partial GitHub merge observations. Adaptive batching is opt-in through `adaptive_batching = true` and requires `max_active_batches = 1`. These features do not authorize merges. `--cancel-stale` requests cancellation of known invalidated runs; capacity remains reserved until a separate safe release.

Use a second terminal with the same database to inspect `sluss --db /secure/sluss.sqlite status`, `queue`, `dispatches`, `ci-evidence --batch-id <id>`, `adaptive-history` and `metrics`. `pause` stops new scheduling; it does not by itself cancel running jobs or release their reservations. Resume with `resume`. Do not retry an uncertain send blindly; inspect `dispatches` and follow the recovery commands described in the [README](../README.md).

To adjust the order of future ordinary batches, use `sluss --db /secure/sluss.sqlite priority --pr 123 --value 10`; set the value to zero to clear the override. Existing batches and bisection order do not change.

Measure a pilot independently with the cohort, completeness and attribution requirements in [benchmarking](benchmark.md). The reported local data is partial and does not by itself establish runner-minute savings or ready-to-main latency.

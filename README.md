# Sluss

Sluss is intended to be a small, open-source CI orchestrator and merge queue for repositories with limited build capacity.

> Run CI for the code you're actually going to merge. Batch the work, not the risk.

## Status: early development

The current code contains a durable SQLite queue of PR **observations**, a batch state machine, resource reservations, a local Git integration builder with an experimental staging API, authenticated GitHub webhook intake, scoped GitHub App authentication and an experimental observation worker. It does **not** automatically start or verify CI, publish integrations, or update target branches. The staging API can explicitly create a temporary staging branch; it is not wired into the worker. It is not ready for production or a pilot. A `Verified` state in the model is not a merge authorization. `doctor` exits unsuccessfully until the real checks exist.

```sh
cargo test
cargo run -- status
cargo run -- queue
cargo run -- metrics
cargo run -- pause
cargo run -- resume
cargo run -- doctor
```

`sluss doctor --config /path/to/sluss.toml --capacity /path/to/capacity.toml --workflow-id <id>` inspects the configured branch, active rules, workflow registration, repository runner inventory and App token scopes. It still exits unsuccessfully because those observations do not establish all prerequisites for strict landing. It has only been tested with a local GitHub stub.

To try webhook intake locally, set `SLUSS_WEBHOOK_SECRET` in the environment and run `cargo run -- serve`. It listens on `127.0.0.1:8080` by default; `--listen` changes the address. External traffic requires a trusted HTTPS reverse proxy. Without configuration, this only records deliveries.

To enable the experimental observer, copy [examples/sluss.toml](examples/sluss.toml) to an operator-controlled location, set `SLUSS_WEBHOOK_SECRET` and `SLUSS_GITHUB_PRIVATE_KEY_PATH`, then run `cargo run -- serve --config /path/to/sluss.toml`. Startup and periodic scans recover missed notifications, and failed work remains queued for retry. Only the configured repository is processed. This path has only been tested with a local GitHub stub; do not deploy this preview as a production GitHub App.

Add `--auto-plan` to the configured service to create batches automatically from its reconciled queue. The queue configuration controls maximum batch size, concurrent active batches, debounce and collection window; see [the example](examples/sluss.toml). The first automatic planning pass binds these settings to the database, and conflicting manual or automatic planners fail closed. Changing bound settings requires an explicit migration, which is not implemented yet. `sluss status` shows active batch IDs and states after restart. With planning alone, batches stop at `Planned`; no batch authorizes a merge.

For experimental automatic staging on GitHub.com, add `--auto-stage --policy-file .github/workflows/sluss-ci.yml` to `serve`, repeating `--policy-file` for every operator-trusted policy file. Sluss refreshes admission, fetches the target and [GitHub PR head refs](https://docs.github.com/en/pull-requests/how-tos/review-pull-requests/checking-out-pull-requests-locally), compares their SHA values with the frozen batch, and builds in disposable Git repositories before publishing the staging ref with a separately scoped App token. Policy paths are bound to the database; conflicting lists fail closed and migration is not yet implemented. Staging requires trusted branch rules that constrain the App, isolated runners, and operator control of the complete policy-file list. This path has been tested with local Git repositories and a GitHub API stub, but fetching and publishing with a real GitHub App have not been validated. Automatic staging does not start CI or authorize landing.

For experimental cancellation of known runs after their batches are invalidated, add `--cancel-stale --capacity /path/to/capacity.toml` to `serve --config ...`. Sluss requests normal GitHub cancellation once and keeps capacity reserved; a missing response remains uncertain and is not blindly retried. `sluss cancel-stale` performs the same check manually. Neither path has been tested against a real GitHub App or runner.

For development, `sluss reconcile --help` describes a manual, one-shot reconciliation command. It requires `SLUSS_GITHUB_PRIVATE_KEY_PATH` and explicit, operator-controlled App, installation and repository IDs. It reads all open PRs and their reviews, checks reviewer permissions, and atomically updates local queue observations if no newer event arrived. This path has only been tested with a local GitHub stub. A database is bound to one repository and policy; a policy change fails closed and currently requires an explicit migration that is not yet implemented.

For an experimental local Git exercise, reconcile a repository first, then freeze an observed base and prepare an integration in a local bare staging repository:

```sh
sluss --db sluss.sqlite plan --id batch-0123456789abcdef --base-sha <base-sha> --debounce-seconds 60 --batch-window-seconds 60
sluss --db sluss.sqlite prepare-local --batch-id batch-0123456789abcdef --source /path/to/source --staging-remote /path/to/staging.git --policy-file .github/workflows/ci.yml
```

The manual planner waits for a partial batch until the oldest selected candidate has been stable for the debounce interval plus the collection window. A full batch and bisection halves do not wait for the collection window. The timestamps survive restarts and depend on the host clock; the optional automatic planner uses the same rules, but is not yet connected to automatic dispatch.

The operator must obtain the target base SHA and list every policy file; the planner rejects a base different from the last successful GitHub reconciliation. That observation can become stale immediately; manual dispatch refreshes it, and strict landing will require a separate final check. The source must contain the frozen commit objects. Preparing a batch does not start CI or grant merge permission. The local staging remote must be trusted and must not trigger unintended workloads. This path is for development and has not been validated against a live GitHub App.

A prepared GitHub staging batch can be dispatched explicitly with `sluss dispatch-github --batch-id <id> --config /path/to/sluss.toml --capacity /path/to/capacity.toml --pool rust=1 --workflow-id <id> --required-job test`. The capacity file describes global pool limits and physical machines; see [the example](examples/capacity.toml). The selected host's unique `sluss-host-*` label is passed as the workflow input in [this single-job example](examples/github-actions/sluss-ci.yml). `sluss observe-dispatch --help` describes how to compare the reported runner against the host's configured runner IDs; `sluss serve --config /path/to/sluss.toml --capacity /path/to/capacity.toml --observe-dispatches` can periodically observe known receipts and retry failed reads after restart; `sluss cancel-dispatch` only releases a reservation that has not been claimed. This path has been exercised with a local GitHub stub, not GitHub or real runners. Before dispatch, Sluss refreshes PR and review admission, requires the bound repository policy and target base to match, and checks the target branch around its staging-ref read. These non-atomic provider reads cannot prevent a later change and do not authorize landing. Sluss also reads the repository runner register using a separate Administration-read App token and compares it with the configured IDs and labels. Organization and enterprise runners are not covered, and physical placement remains operator-supplied. Correct host labels, trusted workflow contents, isolated runners and rerun restrictions remain operator prerequisites; observation does not release capacity or authorize landing.

For a conservative selective-verification preview, `sluss select --policy /path/to/verification.toml --source /path/to/repo --base-sha <base> --head-sha <integration>` compares two commit trees and prints affected suites. See [the policy example](examples/verification.toml). Unknown paths and changes to CI or Cargo manifests select the full configured suite; the operator must maintain the complete dependency graph and policy outside PR-controlled paths.

After staging, `sluss select-batch --batch-id <id> --policy /path/to/verification.toml --source /path/to/repo` freezes the selected suites with the base, integration SHA and policy digest. The source repository must contain both commits, including the staged commit. Supply the same policy as `--verification-policy /path/to/verification.toml` to `dispatch-github`; a changed policy or batch stops dispatch. Without a frozen selection, dispatch passes `full`. The workflow receives the sorted, comma-separated suite names as `sluss_suites`. The example workflow intentionally supports only `full` and fails for selective inputs: an operator must implement and protect their own trusted suite mapping before using selective dispatch. A selection and its input do not establish workflow trust, constitute CI attestation, or authorize landing. Existing preview dispatch records without suite names fail closed and need explicit recovery.

The expected GitHub job names are frozen in each dispatch. `observe-dispatch` compares the provider result with those names, the exact run and commit, the reserved host and the current batch; it persists the comparison but does not issue `sluss/verified`. Trusted workflow policy and branch protections are not yet established.

`sluss metrics` sums actual start-to-finish time for completed, host-checked jobs observed by Sluss. Repeated reads of the same job do not double count it. This is a partial observation set: it excludes jobs Sluss has not observed, unverified placement and rerun attempts, and does not yet report delivered PRs or per-PR savings.

The database defaults to `sluss.sqlite` in the current directory; use `--db PATH` to select another. The pause flag and observations survive restarts; pause also stops the experimental batch planner. Queue entries must eventually be reconciled against GitHub; an observation is never an approval.

## Intended operation

* **CI-only:** schedule existing workflows without changing merge behavior. A batch result is not a per-PR merge approval.
* **Strict merge:** build an integration on a frozen base and PR heads, verify that exact commit, recheck all authorization and policy, and update the target only if the update preserves the required invariants. No silent fallback to squash or ordinary GitHub merges.

Sluss must remain independent of any specific repository. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), and [security](docs/security.md) for the design and open work.

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT), at your option.

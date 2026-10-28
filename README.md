# Sluss

Sluss is intended to be a small, open-source CI orchestrator and merge queue for repositories with limited build capacity.

> Run CI for the code you're actually going to merge. Batch the work, not the risk.

## Status: early development

The current code contains a durable SQLite queue of PR **observations**, a batch state machine, resource reservations, a local Git integration builder with an experimental staging API, authenticated GitHub webhook intake, scoped GitHub App authentication and an experimental observation worker. It does **not** automatically start or verify CI, publish integrations, or update target branches. The staging API can explicitly create a temporary staging branch; it is not wired into the worker. It is not ready for production or a pilot. A `Verified` state in the model is not a merge authorization. `doctor` exits unsuccessfully until the real checks exist.

```sh
cargo test
cargo run -- status
cargo run -- queue
cargo run -- pause
cargo run -- resume
cargo run -- doctor
```

To try webhook intake locally, set `SLUSS_WEBHOOK_SECRET` in the environment and run `cargo run -- serve`. It listens on `127.0.0.1:8080` by default; `--listen` changes the address. External traffic requires a trusted HTTPS reverse proxy. Without configuration, this only records deliveries.

To enable the experimental observer, copy [examples/sluss.toml](examples/sluss.toml) to an operator-controlled location, set `SLUSS_WEBHOOK_SECRET` and `SLUSS_GITHUB_PRIVATE_KEY_PATH`, then run `cargo run -- serve --config /path/to/sluss.toml`. Startup and periodic scans recover missed notifications, and failed work remains queued for retry. Only the configured repository is processed. This path has only been tested with a local GitHub stub; do not deploy this preview as a production GitHub App.

For development, `sluss reconcile --help` describes a manual, one-shot reconciliation command. It requires `SLUSS_GITHUB_PRIVATE_KEY_PATH` and explicit, operator-controlled App, installation and repository IDs. It reads all open PRs and their reviews, checks reviewer permissions, and atomically updates local queue observations if no newer event arrived. This path has only been tested with a local GitHub stub. A database is bound to one repository and policy; a policy change fails closed and currently requires an explicit migration that is not yet implemented.

For an experimental local Git exercise, reconcile a repository first, then freeze an observed base and prepare an integration in a local bare staging repository:

```sh
sluss --db sluss.sqlite plan --id batch-0123456789abcdef --base-sha <base-sha> --debounce-seconds 60
sluss --db sluss.sqlite prepare-local --batch-id batch-0123456789abcdef --source /path/to/source --staging-remote /path/to/staging.git --policy-file .github/workflows/ci.yml
```

The operator must independently obtain the target base SHA and list every policy file; the source must contain the frozen commit objects. Preparing a batch does not start CI or grant merge permission. The local staging remote must be trusted and must not trigger unintended workloads. This path is for development and has not been validated against a live GitHub App.

The database defaults to `sluss.sqlite` in the current directory; use `--db PATH` to select another. The pause flag and observations survive restarts; pause also stops the experimental batch planner. Queue entries must eventually be reconciled against GitHub; an observation is never an approval.

## Intended operation

* **CI-only:** schedule existing workflows without changing merge behavior. A batch result is not a per-PR merge approval.
* **Strict merge:** build an integration on a frozen base and PR heads, verify that exact commit, recheck all authorization and policy, and update the target only if the update preserves the required invariants. No silent fallback to squash or ordinary GitHub merges.

Sluss must remain independent of any specific repository. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), and [security](docs/security.md) for the design and open work.

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT), at your option.

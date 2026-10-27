# Sluss

Sluss is intended to be a small, open-source CI orchestrator and merge queue for repositories with limited build capacity.

> Run CI for the code you're actually going to merge. Batch the work, not the risk.

## Status: early development

The current code contains a durable SQLite queue of PR **observations**, a batch state machine, authenticated GitHub webhook intake, GitHub App token generation and a small local CLI. There is no GitHub reconciliation worker yet; it does **not** admit PRs, start or verify CI, create integration commits, or update branches. It is not ready for production or a pilot. A `Verified` state in the model is not a merge authorization. `doctor` exits unsuccessfully until the real checks exist.

```sh
cargo test
cargo run -- status
cargo run -- queue
cargo run -- pause
cargo run -- resume
cargo run -- doctor
```

To try webhook intake locally, set `SLUSS_WEBHOOK_SECRET` in the environment and run `cargo run -- serve`. It listens on `127.0.0.1:8080` by default; `--listen` changes the address. External traffic requires a trusted HTTPS reverse proxy. Deliveries wake a durable reconciliation queue, but no worker consumes it yet. Do not deploy this preview as a production GitHub App.

The database defaults to `sluss.sqlite` in the current directory; use `--db PATH` to select another. The pause flag and observations survive restarts, but there is no scheduler to pause yet. Queue entries must eventually be reconciled against GitHub; an observation is never an approval.

## Intended operation

* **CI-only:** schedule existing workflows without changing merge behavior. A batch result is not a per-PR merge approval.
* **Strict merge:** build an integration on a frozen base and PR heads, verify that exact commit, recheck all authorization and policy, and update the target only if the update preserves the required invariants. No silent fallback to squash or ordinary GitHub merges.

Sluss must remain independent of any specific repository. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), and [security](docs/security.md) for the design and open work.

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT), at your option.

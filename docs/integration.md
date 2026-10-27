# Local integration builder

The experimental Rust API constructs an ordered sequence of merge commits from a local repository, a frozen base SHA and one or more frozen PR-head SHAs. It creates its own disposable clone and does not write to the source or push any ref. All input objects must be present and fetchable from the source. Only full lowercase SHA-1 commit IDs are accepted. Conflicting or already-contained heads cause the entire operation to fail.

Each merge uses Git's `merge-tree` and creates a two-parent commit with fixed author, committer, date and message. Identical inputs are reproducible for the same Git implementation and merge environment; Git versions or merge behavior can change results, so the deployment must pin and attest those inputs. Generated objects exist in the owned temporary repository only for the lifetime of the result. The caller must preserve them before dropping it if needed.

The builder does not resolve approvals, validate policies, run CI, publish a staging branch or land changes. Tests on generated commits do not authorize a target update. Interrupted-process cleanup and production integration remain open work.

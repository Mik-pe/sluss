# Security

Sluss must fail closed. The early core is not an authorization service and does not modify GitHub. Do not install it with branch bypass privileges or use its current states as merge evidence.

The GitHub App's credentials, webhook secret, signing keys and any release secrets must not be accessible to code running in PR CI jobs. Persistent self-hosted runners can be compromised by untrusted workloads; deployment must use an isolation and cleanup strategy appropriate to the trust level of the code.

Required design work includes authenticating webhook payloads before persistence, installation-scoped access, replay protection and durable deduplication, explicit trusted workflow and check identity, least privilege, auditable evidence, policy-change invalidation, and concurrency-safe landing. Operator-controlled policies and GitHub rules must prevent PR code from impersonating the attestor or disabling required verification. Doctor must validate relevant permissions and rules, while landing must still revalidate them.

Webhook intake verifies HMAC over the raw payload and deduplicates normal GitHub redelivery IDs. GitHub's signature covers the body, not delivery or event headers, so an intercepted valid body and signature can be replayed with a different ID; deduplication alone is not cryptographic replay protection. Treat all events as hints and reconcile current state, use HTTPS at a trusted proxy, and do not expose the webhook secret or captured signed payloads. A reconciliation worker and periodic recovery of missed events remain to be implemented.

The App authentication helper requests read-only access to one repository, rejects unexpectedly broad returned permissions or repository scope, and does not follow HTTP redirects. Its token issuance has only been tested against a local stub, not a GitHub App installation. The caller must verify that an installation and repository are authorized before requesting a token; an untrusted webhook is not sufficient authorization.

PR observations fetch all review pages and compare repeated reads, but GitHub does not expose an atomic snapshot across PRs, reviews and policy. Agreement between reads is not proof that the result stays current. Reviewer authority, branch policy, related PRs and code-owner requirements still need to be checked; observations alone cannot admit or land a change.

No operation may silently fall back from exact-commit strict landing to squash or a different commit. If provider guarantees, evidence, policy identity, or update outcome are uncertain, stop and reconcile rather than land.

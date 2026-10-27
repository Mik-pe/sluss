# Security

Sluss must fail closed. The early core is not an authorization service and does not modify GitHub. Do not install it with branch bypass privileges or use its current states as merge evidence.

The GitHub App's credentials, webhook secret, signing keys and any release secrets must not be accessible to code running in PR CI jobs. Persistent self-hosted runners can be compromised by untrusted workloads; deployment must use an isolation and cleanup strategy appropriate to the trust level of the code.

Required design work includes authenticating webhook payloads before persistence, installation-scoped access, replay protection and durable deduplication, explicit trusted workflow and check identity, least privilege, auditable evidence, policy-change invalidation, and concurrency-safe landing. Operator-controlled policies and GitHub rules must prevent PR code from impersonating the attestor or disabling required verification. Doctor must validate relevant permissions and rules, while landing must still revalidate them.

No operation may silently fall back from exact-commit strict landing to squash or a different commit. If provider guarantees, evidence, policy identity, or update outcome are uncertain, stop and reconcile rather than land.

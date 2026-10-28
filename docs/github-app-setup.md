# GitHub App setup for the development preview

Sluss uses its own GitHub App. A GitHub OAuth token, the Codex GitHub connector or an installation of another App cannot be substituted for that App's private key. The following setup has not yet been verified end-to-end with a real Sluss App; do not grant branch bypass or use this preview to approve or land merges.

Generate a registration link for a private App on your account:

```sh
sluss app-registration-url --name "My Sluss Preview"
```

The default `observe` profile asks for read access to Actions, Checks, Contents, Pull requests and repository Administration. The App can observe the configured repo, and Administration read is used for runner inventory and branch diagnostics. To install on an organization instead, add `--organization <org>`. The account or organization that owns a private App controls where it can be installed. The name must be available on GitHub.

For the experimental staging and CI-dispatch commands, request the separate `ci` profile:

```sh
sluss app-registration-url --name "My Sluss CI" --profile ci
```

It requests write access to Actions, Checks and Contents, plus Pull requests and Administration read. Contents write can update repository refs wherever GitHub rules allow, and Actions write can start and cancel workflows. Configure restrictions and isolate the App's key before using this profile. The App does not need user OAuth authorization and should not have branch bypass for this preview. Separate short-lived, repository-scoped installation tokens are requested for individual operations.

The generated link does not request Workflows write. GitHub can require that permission when a Git push modifies Actions workflow files; such a change may therefore be rejected during staging. Do not add that privilege merely to bypass the preview's trusted-policy checks. Review workflow changes through a separate controlled process.

The generated link opens GitHub's registration page; review the actual selected values before submitting. GitHub may change the form and the link does not prove what was registered. Create the App in the UI, generate a private key there, and store it outside the repository with access restricted to the Sluss process. Install the App only on the intended test repository. The installation ID appears in the installation's GitHub URL; the App's **client ID** is on its settings page. Get the repository's numeric ID, for example, with `gh api repos/OWNER/REPO --jq .id` using a suitably authorized account. Copy [the example configuration](../examples/sluss.toml) outside the repository and fill in those identities and the target branch.

By default the link disables webhooks. A configured service still performs periodic scans; it needs `SLUSS_WEBHOOK_SECRET` set locally even when no webhook is enabled. For a webhook-enabled registration, supply an HTTPS endpoint that reaches the service's `/webhook` route through a trusted proxy:

```sh
sluss app-registration-url --name "My Sluss Preview" --webhook-url https://ci.example.org/webhook
```

GitHub does not allow a webhook secret to be set through registration URL parameters. Set a strong secret separately in the GitHub UI, and provide the same value as `SLUSS_WEBHOOK_SECRET` to Sluss; never put it in the URL. The generated link requests repository events Sluss handles. Organization membership and team events require additional organization Members read access and explicit subscriptions; they are not included in the generated link. Missing, delayed or replayed webhooks must not be treated as authoritative security state.

Start with automatic staging and dispatch disabled:

```sh
export SLUSS_GITHUB_PRIVATE_KEY_PATH=/secure/path/github-app.pem
# Set SLUSS_WEBHOOK_SECRET securely in the service environment.
sluss --db /secure/path/sluss.sqlite serve --config /secure/path/sluss.toml
```

Use `sluss status`, `sluss queue` and the read-only diagnostics to inspect observations. The process and database must be isolated from all code executed by CI runners. Do not place keys, secrets or the operator-owned configuration in PR-controlled files. No live-provider or runner acceptance is implied by generating this link; the current preview cannot publish trusted positive attestation or update the target branch.

References: [GitHub App registration URL parameters](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-using-url-parameters), [GitHub App permission selection](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/choosing-permissions-for-a-github-app), and [webhook events and payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads).

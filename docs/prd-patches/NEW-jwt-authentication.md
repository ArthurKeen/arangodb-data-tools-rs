# PRD patch — APPLIED — new requirement: JWT authentication

**delta_type:** new-requirement
**review_state:** ACCEPTED and applied 2026-10-05
**created:** 2026-10-05
**relates to:** REQ-002 (§8.1 credential sources), REQ-110 (§13.1 common CLI options)

## Observed

JWT authentication was implemented in commit `28260e8` with four selectable
credential modes, token expiry handling, and conflict detection. The PRD
describes none of this.

§8.1 currently asks only that the tools "support password via environment
variable, prompt, or secrets provider hook" (REQ-002), and §13.1 lists
`--username` / `--password-env` / `--password-prompt` / `--auth-token-env`
among the common options (REQ-110). Neither says anything about:

- obtaining a token rather than being handed one,
- the distinction between a **user** JWT and a **superuser** JWT,
- token expiry and refresh, or
- what must happen when several credential sources are supplied at once.

The work was driven by a field report: the AI team could not get `arangoimport`
to use JWT while walking a customer through it — ArangoDB's documentation is
ambiguous there and conflicts across its own client tools — and fell back to
calling the HTTP API directly.

## Proposed patch

Add to §8.1, as new requirements:

> - Support obtaining a JWT, not merely accepting one: by logging in at
>   `POST /_open/auth` with a username and password, and by minting a superuser
>   JWT locally from the server's JWT secret (the equivalent of ArangoDB's
>   `--server.jwt-secret-keyfile`).
> - Treat token expiry as a correctness requirement, not an operational detail.
>   `/_open/auth` issues one-hour tokens on ArangoDB 3.12, which is shorter than
>   many dump and import runs. Any mode in which the tool can re-obtain a
>   credential must refresh it before it lapses and retry once after a 401. A
>   mode in which it cannot (a caller-supplied token) must document that.
> - Accept exactly one credential source per invocation. Supplying two is an
>   error naming both, never a precedence rule the user is expected to learn.
> - Report an authentication failure with the mode that failed and what to
>   check. ArangoDB answers 401 identically whether the secret is wrong, a
>   required claim is missing, or the user lacks rights, so relaying the
>   server's message alone is not actionable.

Add to §13.1's common options: `--jwt-secret-file`, `--jwt-secret-env`, and
`--auth basic|jwt`.

Add to §16.2: live authentication tests covering both JWT modes and both
failure messages, gated on an endpoint being configured.

## Justification

This is a capability the PRD did not anticipate, raised by a customer-facing
failure of the tool this project is modeled on. Recording it as a requirement
makes the behavior testable and keeps §8.1 from implying that accepting a
pre-made token is the whole of JWT support.

The claim requirements are not inferred from documentation. They were verified
against ArangoDB 3.12.4 by signing tokens by hand against a server with a known
JWT secret; `docs/authentication.md` records what was observed, including that a
superuser token requires **both** `server_id` and `iss: "arangodb"` — omitting
`server_id` yields a 401 that gives no indication a claim is missing.

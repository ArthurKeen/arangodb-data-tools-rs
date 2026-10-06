# Proposed PRD patches awaiting review

PRD patches normally live in the shared-memory `prd_patches` collection and are
reviewed from there. These files exist because the memory MCP server was
unreachable when they were raised, and a proposal that only exists in a session
transcript is a proposal that gets lost.

**Nothing here has been applied to `RUST_ARANGODB_TOOLS_PRD.md`.** Per the
project's automation policy, PRD patches are never auto-applied — they require
explicit acceptance. Once the MCP server is reachable, these should be loaded
into `prd_patches` with `review_state: proposed` and this directory emptied.

| File | Requirement | Type | Raised |
|---|---|---|---|
| [REQ-110-output-format.md](REQ-110-output-format.md) | REQ-110 / §13.1 | wrong-signature | 2026-10-05 |
| [NEW-jwt-authentication.md](NEW-jwt-authentication.md) | new | new-requirement | 2026-10-05 |

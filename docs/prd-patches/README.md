# PRD patches raised outside the shared-memory system

PRD patches normally live in the shared-memory `prd_patches` collection. These
files exist because the memory MCP server was unreachable when they were
raised, and a proposal that only exists in a session transcript is a proposal
that gets lost.

Both were **accepted and applied** to `RUST_ARANGODB_TOOLS_PRD.md` on
2026-10-05. They are kept here as the written record of what changed and why,
and should be loaded into `prd_patches` with `review_state: accepted` once the
MCP server is reachable, after which this directory can be emptied.

| File | Requirement | Type | State |
|---|---|---|---|
| [REQ-110-output-format.md](REQ-110-output-format.md) | REQ-110 / §13.1 | wrong-signature | applied 2026-10-05 |
| [NEW-jwt-authentication.md](NEW-jwt-authentication.md) | new (§8.1, §13.1, §16.2) | new-requirement | applied 2026-10-05 |

[sync-2026-10-05.md](sync-2026-10-05.md) records the deferred audit that
accompanied them.

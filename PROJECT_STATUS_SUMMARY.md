# ArangoDB Data Tools (Rust) — Project Status Summary

**Project Status:** MVP Complete (Phases 0–4); Remaining work: Phases 5–7

---

## What's Done ✅

| Phase | Title | Status | Key Deliverables |
|-------|-------|--------|------------------|
| **0** | Foundations | ✅ Complete | Error taxonomy, retry, bounded pipeline, work queue, progress events, config, manifest types, Docker integration harness |
| **1** | Import MVP | ✅ Complete | JSONL/JSON/CSV/TSV readers, local/gzip/zstd support, batch sender pool, duplicate modes, collection creation, edge validation, `arangox-import` CLI, benchmark harness |
| **2** | Object Storage | ✅ Complete | S3-compatible backend (via `object_store` crate), local filesystem, URI parsing, multipart writes, streaming ranged reads, `put_if_absent`, listing |
| **3** | Export MVP | ✅ Complete | Cursor-based collection/AQL export, JSONL/JSON/CSV formats, compression, split, manifest, parallel export, `arangox-export` CLI |
| **4** | Dump & Restore MVP | ✅ Complete | Single-server inventory/metadata/data dump, `/_api/replication` API, manifest-driven restore, index ordering, dependency resolution, `arangox-dump`/`arangox-restore` CLIs |

**Current public API surface:** `arangodb-import`, `arangodb-export`, `arangodb-dump`, `arangodb-restore` stable; library builders fully documented.

---

## What's Remaining 🚀

### Phase 5: Multi-Database, Resume Hardening, Splitting (6–8 weeks)

| Task | Status | Rationale |
|------|--------|-----------|
| **5.1 All-Databases Dump** | Not started | Production use case: dump all DBs in one operation; manifest partitions by database |
| **5.2 Import Resume** | Not started | Large imports can fail halfway; checkpoint-driven continuation avoids duplicates |
| **5.3 Restore Resume** | Not started | Large restores (hours) need checkpoint continuation; critical for large deployments |
| **5.4 Large-Object Split** | Not started | 10 GB export → 100 MB parts; interrupt at part boundary, not mid-upload |
| **5.5 Adaptive Batching** | Not started | Server rate-limit (429) → reduce concurrency; slow recovery → increase; throughput optimization |
| **5.6 Collection Filters** | Not started | Dump subset of DB by regex pattern; restore sees filtered manifest only |
| **5.7 Retry Tuning** | Not started | Configurable backoff; telemetry-driven defaults |

**Why Phase 5 first:** Checkpoint infrastructure gates all resume work; enables production large-scale scenarios.

### Phase 6: RDF Import MVP (4–6 weeks)

| Task | Status | Rationale |
|------|--------|-----------|
| **6.1 RDF Parsers** | Not started | N-Triples, Turtle, N-Quads (streaming); crate choice spike pending |
| **6.2 Graph Model** | Not started | Deterministic key generation (SHA-256), literal policies, bulk load |
| **6.3 CLI** | Not started | `arangox rdf import` subcommand |

**Why Phase 6 next (after Phase 5):** Independent; many users need RDF bulk load; can run in parallel with Phase 7.

### Phase 7: Cloud Backends (3–4 weeks)

| Task | Status | Rationale |
|------|--------|-----------|
| **7.1 GCS** | Not started | Google Cloud Storage support via `object_store` adapter |
| **7.2 Azure** | Not started | Azure Blob Storage support |
| **7.3 SeaweedFS** | Not started | S3-compatible endpoint docs; nightly CI optional |
| **7.4 Cross-backend CI** | Not started | Test matrix (local, S3, GCS, Azure) for all operations |
| **7.5 Backend Docs** | Not started | Setup guides, performance notes, troubleshooting |

**Why Phase 7 last:** All backends reuse stable storage trait; feature gates for optional dependencies.

---

## Critical Path & Parallelization

```
Phase 0 → 1 → 2 → 3 → 4 ✅
                    ↓
                Phase 5 (checkpoint infrastructure)
                 ↙    ↘
           Phase 6   Phase 7
          (RDF)    (Cloud)
           ↓        ↓
          Merge → Final CI/Docs
```

- **Serial (critical path):** Phases 0–5 (checkpoint work gates resume).
- **Parallel:** Phase 6 (RDF) and 7 (cloud backends) after Phase 5 foundations.
- **Estimated total:** 13–18 weeks (5 weeks complete, 8–13 remaining).

---

## Quick Start for Implementation

1. **Read the detailed plan:** [`IMPLEMENTATION_PLAN_REMAINING.md`](IMPLEMENTATION_PLAN_REMAINING.md)
2. **Pick the first task:** Phase 5.1 (all-databases dump) or 5.2 (import resume).
3. **Create feature branch:** `feat/phase-5.1-all-databases` or similar.
4. **Write tests first:** unit tests for checkpoint/manifest changes; integration test with Docker.
5. **Integrate incrementally:** checkpoint types → manifest shape → dump logic → restore logic.
6. **Benchmark:** measure throughput before/after (should be neutral).
7. **Docs:** update `docs/dump-format.md`, `docs/resume.md`, CLI help text.

---

## Key Design Decisions (Recorded for Reference)

### Phase 0–4 (Complete)

1. **Async-first, bounded everywhere:** every pipeline stage has backpressure via bounded channels + global in-flight-byte semaphore.
2. **Library first, CLI second:** all logic lives in typed builders; CLI is thin adapter.
3. **Storage abstraction:** nothing above `arangodb-storage` knows local vs. S3.
4. **Manifest is canonical:** no filename guessing; manifest is the source of truth.
5. **`object_store` crate for S3/GCS/Azure:** proven, correct, covers PRD §10 except restart-resumable multipart (deferred to Phase 5.5).

### Phase 5–7 (To Decide)

1. **RDF crate:** spike to choose `oxrdf`/`oxttl` vs `rio` (streaming N-Triples/Turtle).
2. **Checkpoint location:** independent of dump/import source (separate writable path); clear error if unavailable.
3. **Restart-resumable multipart:** deferred to Phase 5.5; backend-specific (S3 upload ID + parts); for now, each part = full object.

---

## Testing Pyramid (Phases 5–7)

| Layer | Coverage | Phases |
|-------|----------|--------|
| Unit | Checkpoint serialization, key generation, backoff math, regex matching | 5, 6 |
| Integration (Docker) | Resume scenarios, split/concatenate, all-DB dump/restore, RDF parse+load, filters | 5, 6 |
| Storage | Round-trip per backend (local, S3, GCS, Azure, SeaweedFS) | 5, 7 |
| Chaos | Kill dump/restore mid-operation; verify resume from checkpoint | 5 |
| Benchmark | Throughput before/after tuning; per-backend baseline | 5, 7 |

---

## Documentation Created/Updated

- ✅ **`IMPLEMENTATION_PLAN_REMAINING.md`** — This plan (detailed, actionable, per-task).
- ✅ **`PROJECT_STATUS_SUMMARY.md`** — This summary (executive overview).
- 🔲 **`docs/resume.md`** — NEW (Phase 5): checkpoint semantics, at-least-once guarantees.
- 🔲 **`docs/dump-format.md`** — UPDATE (Phase 5): multi-DB shape, split artifacts.
- 🔲 **`docs/rdf-model.md`** — NEW (Phase 6): graph model, literal policies, key generation.
- 🔲 **`docs/backends.md`** — NEW (Phase 7): per-backend setup, feature matrix.
- 🔲 **`docs/cli-reference.md`** — NEW or UPDATE (all phases): comprehensive CLI flag reference.
- 🔲 **`README.md`** — UPDATE: current status, phase completion badges.

---

## Risk Register (Phases 5–7)

| Risk | Impact | Mitigation |
|------|--------|----------|
| Checkpoint semantics race condition (Phase 5) | Resume duplicates or skips data | Atomic `put_if_absent` for checkpoint files; contiguous-prefix validation; chaos tests |
| RDF crate performance (Phase 6) | Slow parsing; unacceptable for large files | Spike early; benchmark `oxttl` vs `rio`; profile hotspots |
| GCS/Azure auth scope creep (Phase 7) | Over-engineering complex credential flows | Start with env-var/keyfile only; defer SAML/Workload Identity to docs/later phases |
| Multipart restart corner cases (Phase 5.5) | Complex state management, bugs hard to find | Test fixture: interrupt mid-upload, verify resume; don't ship until proven |
| Resume correctness with concurrent senders (Phase 5) | Data duplication/loss on retry | Contiguous-prefix checkpointing; logical-key approach; integration tests with small batch sizes |

---

## Definition of Done per Phase

### Phase 5
- [ ] All resume scenarios pass integration tests (interrupt, verify no duplication/loss).
- [ ] All-DB dump/restore validated against Docker ArangoDB.
- [ ] Large-object split tested with 10+ parts; concatenation transparent.
- [ ] Adaptive batching reduces concurrency under 429; throughput stays positive.
- [ ] Collection filters (regex include/exclude) work end-to-end.
- [ ] CLI flags finalized; help text clear.
- [ ] `docs/resume.md` and `docs/dump-format.md` complete.

### Phase 6
- [ ] N-Triples, Turtle, N-Quads parsers fully functional.
- [ ] Deterministic key generation verified (re-import → no new vertices).
- [ ] All three literal policies tested.
- [ ] 10K+ triple import round-trip passes; counts match expected model.
- [ ] `arangox rdf import` CLI works; help text clear.
- [ ] `docs/rdf-model.md` complete with examples.

### Phase 7
- [ ] GCS, Azure backends round-trip successfully (dump/restore).
- [ ] SeaweedFS documented as working S3-compatible option.
- [ ] CI matrix runs; all backends pass critical operations.
- [ ] Performance baselines recorded (MB/sec, docs/sec per backend).
- [ ] `docs/backends.md` complete with setup guides and examples.

---

## Next Steps (First Actions)

1. **Approve plan:** review this document; confirm sequencing and scope.
2. **Set up tracking:** create GitHub issues for each Phase 5–7 task.
3. **Spike RDF crate:** benchmark `oxttl` vs `rio` in isolation; record results.
4. **Start Phase 5.1:** branch `feat/phase-5.1-all-databases`; implement manifest extensions.
5. **Build CI infrastructure:** ensure Docker ArangoDB is stable in CI; add feature gates for optional backends.

---

## References

- **PRD:** [`RUST_ARANGODB_TOOLS_PRD.md`](RUST_ARANGODB_TOOLS_PRD.md) — user needs, non-goals, crate structure.
- **Implementation Plan (Phases 0–4):** [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — architecture, phase definitions, testing strategy.
- **Remaining Work (Phases 5–7):** [`IMPLEMENTATION_PLAN_REMAINING.md`](IMPLEMENTATION_PLAN_REMAINING.md) — detailed tasks, APIs, exit criteria.
- **Crate READMEs:** each crate documents its role and public API.


# ArangoDB Data Tools (Rust) — Development Roadmap

## Timeline Overview

```
Q1 (Complete) ✅
├─ Phase 0: Foundations
├─ Phase 1: Import MVP
├─ Phase 2: Object Storage (S3)
├─ Phase 3: Export MVP
└─ Phase 4: Dump & Restore MVP

Q2 (In Progress)
├─ Phase 5: Resume Hardening & Multi-DB (6–8 weeks)
│  ├─ Weeks 1–2:  All-database dump + import resume
│  ├─ Weeks 3–4:  Restore resume + collection filters
│  ├─ Weeks 5–6:  Large-object splitting + adaptive batching
│  └─ Weeks 7–8:  Retry tuning + documentation
├─ Phase 6: RDF Import (4–6 weeks, parallel with Phase 5.5+)
│  ├─ Week 1:    Crate evaluation spike
│  ├─ Weeks 2–3: Parsers (N-Triples, Turtle, N-Quads)
│  ├─ Weeks 4–5: Graph model + bulk load
│  └─ Week 6:    CLI + documentation
└─ Phase 7: Cloud Backends (3–4 weeks, parallel with Phase 6)
   ├─ Week 1:    GCS backend + documentation
   ├─ Week 2:    Azure backend + documentation
   ├─ Week 3:    SeaweedFS docs + cross-backend CI matrix
   └─ Week 4:    Performance baselines + final docs
```

---

## Phase 5: Multi-Database, Resume Hardening, Splitting

**Status:** In progress — done: all-database dump + restore (5.1), import resume (5.2), restore resume (5.3), JSONL split (5.4), collection filters (5.6). Remaining: adaptive batching (5.5), retry tuning (5.7), split leftovers (JSON/CSV, multipart restart-resume).  
**Duration:** 6–8 weeks  
**Dependency:** Phase 4 complete (dump/restore working)  
**Owned by:** [TBD]  
**Success criteria:** All resume scenarios work; no data loss on interrupt  

### Week-by-Week Breakdown

#### Weeks 1–2: Foundations (All-Databases Dump + Import Resume)

| Task | Effort | Notes |
|------|--------|-------|
| **All-Databases Dump** | 3 days | Design manifest multi-DB structure; implement `DumpOptions::all_databases` flag; extend dump logic to enumerate `/_api/database`; update manifest artifact paths to include DB name |
| **Import Resume** | 4 days | Design checkpoint types; add `CheckpointKey` enum; implement checkpoint persistence (`put_if_absent`); add `--checkpoint-path` CLI flag; update reader to seek/skip from checkpoint |
| **Documentation** | 1 day | Create `docs/resume.md`; document at-least-once semantics; clarify duplicate-mode interaction |
| **Testing** | 2 days | Unit: checkpoint serialization; Integration: import 1M docs, interrupt at 50%, resume, verify count |

**Deliverable:** All-databases dump + import resume working, tested end-to-end.

#### Weeks 3–4: Restore Resume + Collection Filters

| Task | Effort | Notes |
|------|--------|-------|
| **Restore Resume** | 4 days | Design `RestoreCheckpoint` struct; emit checkpoint after each collection; read/skip on startup; validate checkpoint matches dump |
| **Collection Filters** | 3 days | Add regex include/exclude filters; apply at dump time (inventory filtering); update manifest artifact list; CLI flags: `--include-collections`, `--exclude-collections` |
| **Testing** | 2 days | Integration: dump → interrupt restore at collection 2/4 → resume; verify no duplication; verify filters work |

**Deliverable:** Restore resume + filters working; comprehensive checkpoint testing.

#### Weeks 5–6: Large-Object Splitting + Adaptive Batching

| Task | Effort | Notes |
|------|--------|-------|
| **Large-Object Split** | 4 days | Design `ArtifactPart` struct; extend export/dump to rotate objects at size threshold; update manifest to track parts; restore reader concatenates transparently |
| **Adaptive Batching** | 3 days | Monitor 429/503; reduce concurrency; track RTT; adjust batch size; `--adaptive-batching` CLI flag |
| **Benchmarking** | 2 days | Measure throughput: no split vs 100 MB parts; measure throughput on rate-limited server |
| **Testing** | 1 day | Unit: split logic; Integration: export with 10 MB parts, re-import |

**Deliverable:** Split exports working; adaptive batching measurably improves throughput under load.

#### Weeks 7–8: Retry Tuning + Phase 5 Completion

| Task | Effort | Notes |
|------|--------|-------|
| **Retry Tuning** | 2 days | Configurable backoff (`--max-retry-delay`); exponential backoff with jitter; logging |
| **Phase 5 Integration Tests** | 2 days | Full round-trip: 1M-doc import + resume + all-DB dump + resume restore + filters + split |
| **Documentation** | 2 days | Update `docs/dump-format.md` with multi-DB + split spec; update `docs/cli-reference.md` |
| **Code Review & Polish** | 1 day | Clippy, fmt, unit test coverage |

**Deliverable:** Phase 5 complete; all tasks tested; documentation finalized.

---

## Phase 6: RDF Import MVP

**Status:** Complete — N-Triples/N-Quads/Turtle parsers, deterministic keys, literal policies, RPT/PGT graph models, streaming edges, and the `arangox rdf import` CLI.  
**Duration:** 4–6 weeks  
**Dependency:** Phase 5.2 (import pipeline) or concurrent  
**Owned by:** [TBD]  
**Success criteria:** 10K+ triple import; deterministic keys; all formats working  

### Week-by-Week Breakdown

#### Week 1: RDF Crate Evaluation Spike

| Task | Effort | Notes |
|------|--------|-------|
| **Spike: `oxrdf`/`oxttl` vs `rio`** | 2 days | Benchmark streaming parse of 100K N-Triples; measure memory, CPU; evaluate API for IRI expansion, blank-node handling |
| **Decision & Setup** | 1 day | Choose crate; add to workspace; create parser stubs |

**Deliverable:** RDF crate choice finalized; benchmark recorded.

#### Weeks 2–3: Parsers (N-Triples, Turtle, N-Quads)

| Task | Effort | Notes |
|------|--------|-------|
| **N-Triples Parser** | 2 days | Streaming parser; IRI/blank-node/literal extraction; error handling with line numbers |
| **Turtle Parser** | 2 days | Wrap crate parser; prefix expansion; test fixtures with real Turtle |
| **N-Quads Parser** | 1 day | Extend N-Triples to optional 4th field (named graph) |
| **Testing** | 2 days | Unit: parse fixtures; verify triple count; verify IRI normalization |

**Deliverable:** All three RDF formats parse correctly and consistently.

#### Weeks 4–5: Graph Model + Bulk Load

| Task | Effort | Notes |
|------|--------|-------|
| **Key Generation** | 1 day | Deterministic SHA-256 keys; blank-node salting with file+line |
| **Literal Policy** | 2 days | Implement all three: NoLiterals, EmbedInEdge, MaterializeAsVertex |
| **Bulk Load** | 2 days | Generate vertex/edge `Value` documents; feed to import pipeline |
| **Testing** | 2 days | Integration: import Turtle fixture (10K triples); verify vertex/edge counts; verify deterministic keys (re-import → no new vertices) |

**Deliverable:** RDF graph model working; bulk load pipeline integrated.

#### Week 6: CLI + Documentation

| Task | Effort | Notes |
|------|--------|-------|
| **CLI Integration** | 1 day | `arangox rdf import` subcommand; `--rdf-literal-policy`, `--format`, `--vertex-collection`, `--edge-collection` flags |
| **Error Handling** | 1 day | Malformed RDF → clear error with line number |
| **Documentation** | 2 days | Create `docs/rdf-model.md` with examples; update `docs/cli-reference.md`; help text |
| **Benchmark** | 1 day | Import 100K triples; record throughput (triples/sec, MB/sec) |

**Deliverable:** `arangox rdf import` fully functional; docs complete.

---

## Phase 7: Cloud Backends

**Status:** Not started  
**Duration:** 3–4 weeks  
**Dependency:** Phase 4 complete (storage trait stable)  
**Owned by:** [TBD]  
**Success criteria:** GCS/Azure round-trip working; all backends in CI matrix  

### Week-by-Week Breakdown

#### Week 1: GCS Backend + Documentation

| Task | Effort | Notes |
|------|--------|-------|
| **GCS Backend** | 2 days | Feature gate `gcs`; URI parsing `gs://`; credential handling (GOOGLE_APPLICATION_CREDENTIALS, ADC) |
| **Testing** | 1 day | Integration (nightly): round-trip via GCS; handle credential errors |
| **Documentation** | 1 day | Setup guide (service account, keyfile, env var); example command |

**Deliverable:** GCS backend working; documented.

#### Week 2: Azure Backend + Documentation

| Task | Effort | Notes |
|------|--------|-------|
| **Azure Backend** | 2 days | Feature gate `azure`; URI parsing `azure://` or `abfs://`; credential handling (account key, Managed Identity) |
| **Testing** | 1 day | Integration (nightly): round-trip via Azure |
| **Documentation** | 1 day | Setup guide; example command |

**Deliverable:** Azure backend working; documented.

#### Week 3: SeaweedFS Docs + Cross-Backend CI

| Task | Effort | Notes |
|------|--------|-------|
| **SeaweedFS Documentation** | 1 day | Document as S3-compatible option; `--s3-endpoint` flag; deployment guide |
| **Cross-Backend CI Matrix** | 2 days | Test local, S3/MinIO, GCS, Azure for: import, export, dump, restore, split, resume (if Phase 5 done) |
| **Performance Baselines** | 1 day | Measure MB/sec, docs/sec per backend; record in `docs/backends.md` |

**Deliverable:** SeaweedFS documented; all backends in nightly CI.

#### Week 4: Final Docs + Polish

| Task | Effort | Notes |
|------|--------|-------|
| **Backend Feature Matrix** | 1 day | Document which backends support resumable uploads, multipart, etc. |
| **Troubleshooting Guide** | 1 day | Credential errors, endpoint misconfiguration, common issues per backend |
| **Performance Recommendations** | 1 day | Batch size, concurrency tuning per cloud provider |
| **Code Review & Cleanup** | 1 day | Clippy, fmt, dead-code removal; feature gate tests |

**Deliverable:** Phase 7 complete; comprehensive backend documentation.

---

## Work Items by Priority

### Critical Path (Must Complete in Order)

```
[x] Phase 5.1: All-Databases Dump + multi-database restore
[x] Phase 5.2: Import Resume
[x] Phase 5.3: Restore Resume
[~] Phase 5: Completion (split done for JSONL, filters done; batching + retry tuning remain)
```

### High Value (Can Start After Phase 5)

```
[ ] Phase 6: RDF Import
[ ] Phase 7: Cloud Backends
```

---

## Testing & CI Checkpoints

| Checkpoint | Phase | Gate Condition |
|------------|-------|----------------|
| **All-databases dump** | 5.1 | 4-database dump/restore round-trip passes |
| **Import resume** | 5.2 | 1M-doc import, interrupt, resume; count = 1M; no duplication |
| **Restore resume** | 5.3 | 4-collection dump, interrupt at collection 2, resume; all 4 collections restored exactly once |
| **Large-object split** | 5.4 | Export 100 MB+ in 10 MB parts; manifest lists parts; re-import validates counts |
| **Adaptive batching** | 5.5 | Import under sustained 429; concurrency reduces; throughput positive |
| **Collection filters** | 5.6 | Dump with include filter; manifest lists only matching collections |
| **RDF parsing** | 6.1 | Parse N-Triples, Turtle, N-Quads; verify triple counts |
| **Deterministic keys** | 6.2 | Re-import same RDF; no new vertices created |
| **GCS round-trip** | 7.1 | Dump → restore via GCS; counts match |
| **Azure round-trip** | 7.2 | Dump → restore via Azure; counts match |
| **Cross-backend CI** | 7.4 | All backends pass import/export/dump/restore in nightly CI |

---

## Known Blockers & Dependencies

| Item | Blocks | Mitigation |
|------|--------|-----------|
| RDF crate choice (Phase 6, Week 1) | RDF parser work | Spike early; default to `oxttl`; have `rio` fallback |
| Docker ArangoDB 3.12 stability (Phase 5+) | All integration tests | Ensure testcontainers setup is reliable; pin version |
| S3 testing against MinIO (Phase 2, validated) | Phase 5+ splitting | MinIO already in use; no new infra needed |
| GCS/Azure credentials in CI (Phase 7) | Nightly CI for those backends | Use service accounts; store keys in GitHub Secrets; run nightly only |

---

## Resource Allocation

| Phase | Team Size | Full-Time | Estimated Effort |
|-------|-----------|-----------|------------------|
| 5 | 1–2 | Yes | 6–8 weeks |
| 6 | 1 | Yes | 4–6 weeks (can overlap Phase 5.5) |
| 7 | 1 | Yes | 3–4 weeks (can overlap Phase 6) |

**Suggested assignments:**
- **Phase 5 lead:** Primary engineer (async/concurrency focus).
- **Phase 6 lead:** Secondary engineer (parser/data model focus) or same as Phase 5 after Week 4.
- **Phase 7 lead:** Secondary engineer or contracted (straightforward integration work).

---

## Success Criteria for Each Phase

### Phase 5: "Production-Ready Resume"
- ✅ Interrupt import/dump/restore at any point; resume without loss or duplication.
- ✅ Multi-database dump/restore works end-to-end.
- ✅ Large exports split transparently; no per-part overhead.
- ✅ Server under load (429/503); system adapts gracefully.
- ✅ CLI flags finalized; help text clear; docs complete.

### Phase 6: "RDF Bulk-Load Ready"
- ✅ Parse standard RDF formats (N-Triples, Turtle, N-Quads) correctly.
- ✅ Deterministic key generation verified (same RDF → same vertices).
- ✅ Bulk load 10K+ triples in < 10 seconds.
- ✅ CLI easy to use; error messages helpful.
- ✅ Docs include real examples.

### Phase 7: "Multi-Cloud Ready"
- ✅ All backends (local, S3, GCS, Azure, SeaweedFS) work for dump/restore.
- ✅ Performance parity across backends (±20%).
- ✅ Setup docs tested and working.
- ✅ CI matrix runs nightly; all backends passing.

---

## Post-Phase-7 Roadmap (Future Releases)

### Phase 8 (Future): Cluster Support
- Cluster-topology-aware dump (per-shard checkpoints).
- Parallel restoration with shard-aware recovery.
- Cross-shard consistency guarantees documented.

### Phase 9 (Future): Advanced RDF
- RDF/XML, TriG parsing.
- Configurable IRI normalization / domain-specific key strategies.
- SHACL validation hooks.

### Phase 10 (Future): Enterprise Features
- VelocyPack support (if public format spec available).
- Encrypted backup support (if compatible public format exists).
- Custom transformation hooks (e.g., in-flight data mapping).

---

## Document References

| Document | Purpose | Updated |
|----------|---------|---------|
| [`RUST_ARANGODB_TOOLS_PRD.md`](RUST_ARANGODB_TOOLS_PRD.md) | Product requirements | Phase 4 complete ✅ |
| [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) | Phases 0–4 plan (completed) | Phase 4 complete ✅ |
| [`IMPLEMENTATION_PLAN_REMAINING.md`](IMPLEMENTATION_PLAN_REMAINING.md) | Phases 5–7 detailed plan | NEW 🆕 |
| [`PROJECT_STATUS_SUMMARY.md`](PROJECT_STATUS_SUMMARY.md) | Quick executive summary | NEW 🆕 |
| [`DEVELOPMENT_ROADMAP.md`](DEVELOPMENT_ROADMAP.md) | This file (timeline + tracking) | NEW 🆕 |

---

## Contact & Questions

- **PRD & architecture questions:** see `RUST_ARANGODB_TOOLS_PRD.md`
- **Phase 5–7 detailed questions:** see `IMPLEMENTATION_PLAN_REMAINING.md`
- **Status & progress:** see `PROJECT_STATUS_SUMMARY.md`
- **Timeline & milestones:** see this file


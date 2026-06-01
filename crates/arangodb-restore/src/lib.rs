//! Database restore for ArangoDB.
//!
//! Manifest validation, dependency-ordered collection/index/view restore
//! (distributeShardsLike prototypes, docs before edges, `_analyzers` first,
//! `_users` last, vector indexes after data), and cheap-checkpoint resume. See
//! `docs/IMPLEMENTATION_PLAN.md` (section 4, phases 4 and 5).

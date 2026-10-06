# What I Learned Auditing My Own Rust Project Against Its Spec

### Nine crates, 259 passing tests, and six capabilities that nothing ever called

---

I have been building `arangodb-data-tools-rs`, a Rust toolkit for ArangoDB bulk data work:
import, export, dump, restore, and an RDF loader, all streaming to object storage. Nine
crates, about 13,500 lines, 259 tests passing, clippy clean under `-D warnings`, CI against a
live database.

By every signal I normally trust, it was in good shape.

Then I audited it line by line against its own product requirements document — 130 extracted
requirements, each one classified and each classification required to carry a `file:line`
citation that a script would mechanically verify. The result was humbling in a specific and
useful way: **88 implemented, 24 partial, 17 missing**.

The interesting part was not the count. It was that the audit found the same bug six separate
times, and my test suite was structurally incapable of catching any of them.

## The shape of the defect

Here is the pattern, stated once:

> A capability is designed, implemented, and unit-tested at the type layer — and then never
> called from any production code path.

Six instances, found in one pass:

**`put_if_absent`.** Implemented on both storage backends. Tested on both. Called by nothing.
Manifests and checkpoints are written with unconditional puts, which means two concurrent
dumps to the same object-store prefix silently overwrite each other. The conditional-write
primitive that exists specifically to prevent this sits unused a few modules away.

**View definitions.** The replication inventory's `views` array is parsed into the client
model. `ArtifactKind::View` is defined in the manifest schema. Neither is ever written or
read. A dump of a database with ArangoSearch views produces a dump with no views, and says
nothing about it.

**Three `ErrorContext` builders.** The spec asks that errors carry collection, object path,
byte range, batch number, and server response. All five are modeled. The import sender
populates two. The builders for the other three are called from nowhere, so every storage and
dump error carries an empty context — and the `Display` impl does not render the context
anyway.

**The redaction helper.** Written specifically so that AQL queries and bind variables never
reach a log in the clear. Called from nowhere.

And here is where the audit got one wrong, which is worth more than the finding. I recorded
this as "the requirement holds by accident rather than by enforcement — the request struct
derives `Debug` with the query in plain text." It does not. It has a hand-written `Debug`
that redacts both the query and the bind variables. The protection was real and deliberate;
it just used a local `"<redacted>"` literal instead of the shared constant, so my grep for
the constant found nothing and I inferred absence from it.

That is the failure mode of a grep-driven audit stated precisely: **absence of the symbol you
searched for is not absence of the behavior.** I had the right instinct — go look — and then
skipped the looking for one item out of six because the grep felt conclusive. The fix was to
read the file.

**Two progress counters.** `ProgressCounters` has a `server_errors` field with no increment
method, and an `add_retries` method that nothing calls. Every progress event the tool has
ever emitted reported `server_errors: 0, retries: 0`. Those are not measurements. They are
hard-coded zeros wearing the costume of measurements.

**Two data-format variants** with no writer behind them.

Every one of these passes its tests. Of course it does. **A unit test proves the unit works.
It proves nothing whatsoever about whether anything calls it.** The test suite and the dead
code were written by the same person in the same sitting, sharing the same assumption, and a
test written against code you just authored and that passes on the first run has confirmed
that assumption rather than challenged it.

## The cheapest detector I know

After the audit I went looking for a way to find this class of bug without a 130-requirement
review. The answer turned out to be almost embarrassingly simple:

> Grep for public items in your core and infrastructure crates that have no non-test callers.

That one sweep located roughly six gaps faster than reading feature code did. Dead code
analysis is not new. What is worth saying is *where* the dead code congregates: not in the
feature modules, where unused functions are obvious during review, but in the shared
foundation crates, where an item having "a caller somewhere" is assumed rather than checked.

There is a second signal, and it is the one I would not have predicted. The audit required
every "implemented" claim to carry a `file:line` citation, mechanically verified — the script
checks that the cited line actually contains the term the claim is about. Two of the six
findings surfaced because **a citation that looked perfectly plausible failed that check.**
My instinct was to assume I had mistyped a line number. I had not. The term was absent
because the capability was absent.

That is now a rule I keep: **a failed evidence check is a finding until proven a typo**, not
the other way around.

## Where the spec was wrong, not the code

An audit that only ever finds the code at fault is not an audit; it is a confession. Two
requirements turned out to be obsolete, and both were cases where the implementation had
legitimately outgrown the text.

The spec said import should checkpoint input byte offsets, "only for seekable or
range-readable sources." The implementation instead checkpoints **deterministic batch
indices**. Because identical input and batching configuration yield an identical batch
sequence, a restarted import re-derives the same batches and skips the committed ones. That
works for *any* source — including non-seekable stdin, which the requirement explicitly
excluded. Following the spec literally would have been a regression.

The spec also described five separate binaries, `arangox-import`, `arangox-dump`, and so on.
The tool ships one `arangox` binary with subcommands, which is the idiomatic Rust shape and
lets shared connection flags be defined once instead of five times. That is a good decision —
but it had left **every copy-pasteable example in the spec naming a binary that does not
exist.**

Both became proposed spec patches rather than silent absorptions, and both were applied after
review. The asymmetry matters: when code and spec disagree, the default assumption that the
code is wrong is itself a bug in the process.

## What I would tell someone starting this

**Tests tell you a unit works. Nothing in a test suite tells you a unit is reachable.** If you
want that property, you need a different instrument — a reachability sweep, a coverage report
read for zeros rather than for percentage, or an audit that demands evidence.

**Demand mechanically checkable evidence, not prose.** "Implemented" is a claim. A verified
`file:line` is a fact. The difference between them is where my findings were hiding — and
the one finding I did *not* verify against the file is the one I got wrong.

**Rank gaps by consequence, not by effort.** Of 41 open gaps in my project, exactly one can
lose data without telling you — views silently missing from a dump. The other forty either
fail loudly or are visible in the output. That one outranks all of them, and a backlog sorted
by story points would have buried it.

**Write down what does not work.** The limitations section of my README is now longer and more
specific than it was, and the project is more trustworthy for it, not less. "Pre-alpha" is not
a disclosure. "A dump of a database with views is silently incomplete, and here is the
tracking ID" is a disclosure.

---

`arangodb-data-tools-rs` is pre-alpha and developed in the open:
[github.com/arango-solutions/arangodb-data-tools-rs](https://github.com/arango-solutions/arangodb-data-tools-rs).
The full graded audit, including every open gap, lives in
[`docs/scorecard.md`](https://github.com/arango-solutions/arangodb-data-tools-rs/blob/main/docs/scorecard.md).

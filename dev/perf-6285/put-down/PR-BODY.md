Part of #6285 — evidence from two darts, not for merge: making consumers read a selection directly leaves TPC-H Q1/Q6 SF1 hot times unchanged, while caching each expression spec's per-batch setup cuts Q6 by 11%.

- **Selection-aware vectors don't move Q1/Q6 wall-clock**
  Every variant landed within ±3% of baseline, against ±4% spread between baseline JVMs. Full numbers, profiles and blast radius are in `dev/perf-6285/REPORT-sel-aware-vectors.md`.
- **Caching per-batch expression setup is the measurable win**
  Q6 4245 → 3770 ms (−11%, all three JVM pairs agree); Q1 −4%, within noise. Report in `dev/perf-6285/REPORT-per-batch-setup.md`; the code is a patch under `dev/perf-6285/per-batch-setup/`.
- **One latent bug on `main` turned up, worth landing on its own**
  `RenamedVector` reports its inner vector's name in `field`; see Consequences.

## What changes

The branch is main + the Q1/Q6 harness + two experiments; none of it is meant to land as-is.

- **Vectors can expose a selection instead of hiding it** (commits "vectors expose direct+selIdxs…" and "codegen reads primitive vars as vec.getX(sel[i])…")
  `VectorReader` gains defaulted `direct` and `selIdxs`; an `IndirectVector` over a `Selection` answers with its inner vector and index array. Expression codegen, `Sum`, `Count` and `GroupMapper` read `direct` at `sel[i]`; the emitted loop is specialised on which variables carry a selection, so there is no per-row branch. Codegen also reads primitives as `vec.getX(i)` directly, without a `ValueReader`.
- **`RelationReader.select` composes a selection once per shared index array, not once per column**
  Columns from one source already share one index array by identity (the scan selects the `put` leg once), so an `IdentityHashMap` keeps them sharing through composition.
- **Expression specs resolve their compiled fn once per input shape, and selections emit `int[]` directly** (patch only, applies to main + harness)
  A spec caches `[args, param-types, per-variable types, zone, compiled fn]`; a batch costs one `getType` per referenced variable plus an array compare. Selection specs generate a loop that writes matching row indices into an `int[]`, replacing bool vector → `VectorMask` → `IntStream` → `int[]`. Only active with narrowed var-types.

## Consequences

- **Per-value interface calls drop from 7 to 1, and C2 had already absorbed most of them**
  Measured on a non-null f64 read in generated code. JFR shows the touched phases shrinking within themselves (generated fns −12…−19%, `sum` −23…−27%, `select` −40…−60%), but those phases are ~25% of Q1 and ~17% of Q6; the generated fns' remaining self-time is the loads.
- **Q1/Q6 time is elsewhere**
  Event merge is 27% of Q1 and 42% of Q6. Group-by key mapping is 20–25% of Q1, allocating a `ByteBuffer` per utf8 key hash/compare plus a `byte[]` per hash-trie insert (`MutableMemoryHashTrie.intToByteArray`). Per-batch expression setup is 4% of Q1 and 9.6% of Q6.
- **`RenamedVector.field` carries the inner vector's name**
  It delegates `field` via `by inner`. That is harmless while a rename sits outermost, but an `IndirectVector` wrapping a `RenamedVector` forwards `field` to it and materialises the old name — TPC-H Q22 failed with `missing vector: c_custkey` once `select` composed on `direct`. Fixed on this branch by the commit "RenamedVector reports its own name in field…". No issue has been filed for it yet.
- **Any `VectorReader by inner` delegator must override `direct`/`selIdxs`**
  Otherwise it answers with its inner's. `RenamedVector` (must stay renamed) and `DenseUnionVector.LegVector` (would leak its `LegReader`) are the two that exist today.
- **Measurements were taken on two different snapshot layouts**
  All selection-aware numbers ran in the stale-snapshot layout from #6174 (~90-row batches); the per-batch-setup comparison ran after the harness's one-tx `:refresh-snapshot` stage. The two reports are not comparable with each other.
- **Tests**
  Selection-aware variant: `:test` 1875/1876 (the failure is `xtdb.authn-test` initialisation, from a local no-Docker patch) and `:xtdb-core:test` 601/601. Per-batch-setup patch: operator, expression, tpch and sql namespaces pass with narrowed var-types off and forced on. Integration and property tests were not run.

## Out of scope

- **A landable change**
  The harness commits are marked not-for-landing and the branch carries experiment toggles (`-PnoSelAware`, `-PnoSelRel`, `-PnoSpecCache`, `-PnoDirectSel`, `-Dxtdb.noDirectGet`).
- **`Slice` selections**
  `selIdxs` is null for them, so they keep reading through the `IndirectVector`; not on the Q1/Q6 path.
- **Nested selections**
  A selection over a selection (e.g. a scan with an `_iid` predicate) correctly falls back to the view.
- **Operators other than project, select, scan predicates and group-by sum/count**
  Joins, sort, distinct, window and the remaining aggregates still read through views, unchanged and correct.

## Alternative approaches

- **A relation-level selection type (`IndirectRelation`, the earlier spike on `dart/6285-indirect-relation-scan`)** — reasoned against
  Same composition count as sharing index arrays by identity, but it needs a new `RelationReader` whose `vectors` manufacture views, consumers have to downcast to benefit, and the grouping is lost when `ProjectCursor` or `RelationReader.from` repacks columns.
- **Bitmasks as the selection carrier** — reasoned against, not built
  Every downstream loop would scan all rows; Q6 keeps ~2% of them.

## Open questions

- **Q1: Is the bar wall-clock on Q1/Q6, or fewer calls per value as a structural property?**
  Decides whether the codegen/aggregate parts of the selection-aware change are worth landing at all: on a wall-clock bar they are dropped and only `select`'s compose-once and the `RenamedVector.field` fix remain; on a structural bar they land as `direct`/`selIdxs`, then codegen reading `direct` at `sel[i]`, then `Sum`/`Count`/`GroupMapper` reading direct.
- **Q2: Should sharing an index array across columns be a documented invariant?**
  `select`'s compose-once and the `GroupMapper` direct path need it for their speed-up, not for correctness.
- **Q3: Not measured — the split between the spec cache and direct `int[]` emission, JFR of the per-batch variant, and the per-batch variant on the stale layout.**
  The planned 3×3 run was lost; a rerun with `-PnoDirectSel` against the refreshed layout settles the first.

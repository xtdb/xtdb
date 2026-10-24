# #6285 avenue C — sel-aware vectors (dart, evidence only)

Branch `dart/6285-sel-aware-vectors`: `708148c` harness → `b020110` V1 → `1545988` V3 → `+1` RenamedVector fix.
Couldn't read #6285/#4964/#6163 from this session (xtdb/xtdb not attachable: same-name repo clash with jarohen/xtdb); worked from the harness commit, the predecessor spike and the code.

## TL;DR

- Hoisting the indirection out of the per-value call works and is cheap to build, but **buys nothing measurable on Q1/Q6 wall-clock** (all variants within ±3% of baseline; baseline itself drifts ~6% between JVMs).
- It removes 4–6 interface dispatches per value read, and the profile shows the phases it touches shrinking (codegen −12…−19%, `sum` −23…−27%, `select` −40…−60% of their own samples) — but those phases are ~25% of Q1 and ~17% of Q6, and the dispatches were already mostly devirtualised/inlined by C2.
- The time is elsewhere: event merge (27% Q1 / 42% Q6), group-by key hashing/compare on utf8 (20–25% Q1, allocates a `ByteBuffer` per hash/compare), per-batch expression setup (4% Q1 / 9.5% Q6), page load (5–10%).
- The minimum seam is tiny and non-breaking: two defaulted members on `VectorReader` (`direct`, `selIdxs`) — no new relation type, no consumer *has* to change.

## Variants

- **base**: xtdb/xtdb `main` 418e8b4 + harness, run with `-PnarrowVarTypes`.
- **V1** (`b020110`): `VectorReader.direct`/`selIdxs`; `IndirectVector` over a `Selection` answers them; consumers read `direct` at `selIdxs[i]`:
  - expression codegen (`:variable`): per-var `valueReader` on the *direct* vector, `setPos(aget sel idx)`; which vars carry a sel is part of the `emit-projection` memo key (`:sel-vars`), so the emitted loop is specialised, no per-row branch.
  - `Sum`, `Count`: direct vector's summer / `isNull` at `sel[idx]`.
  - `GroupMapper.Mapper`: if all key cols share one sel (by identity) and `!storeFullBuildRel`, build the relation map over the direct key cols and `addIfNotPresent(sel[idx])`.
  - `RelationReader.select(idxs)`: compose once per *distinct sel array* (`IdentityHashMap<IntArray, IntArray>`) instead of once per column — vectors that shared a sel keep sharing the composed one.
- **V3** (`1545988`): V1 + codegen reads primitive (and nullable-primitive) vars as `vec.getX(sel[i])` directly on the direct vector — no `ValueReader` at all.
- **V3ns**: V3 with `-PnoSelAware -PnoSelRel` — i.e. *only* the direct-`getX` codegen, reading through the `IndirectVector` (isolates "monomorphic direct read" from "sel hoisting").
- Toggles: `-PnoSelAware`, `-PnoSelRel` (V1+), `-Dxtdb.noDirectGet` (V3).

## Numbers

SF1, RA, 50 hot reps/JVM, first 20 dropped, interleaved. Two rounds (round 2 ran ~40 min after round 1; the whole box got ~6% faster in between, hence compare within a round only).

Round 1 — base vs V1 (`runs.py --drop 20`, verbatim):
```
ab1-base-1.log       q1   cold   9393 n  30 median    6724 mean    6717 min   6362 sd    166
ab1-base-1.log       q6   cold   5235 n  30 median    4946 mean    4949 min   4760 sd    101
ab1-base-2.log       q1   cold   8679 n  30 median    6607 mean    6624 min   6244 sd    186
ab1-base-2.log       q6   cold   5166 n  30 median    4848 mean    4860 min   4622 sd    161
ab1-base-3.log       q1   cold   7987 n  30 median    6334 mean    6369 min   6116 sd    190
ab1-base-3.log       q6   cold   5004 n  30 median    4802 mean    4799 min   4549 sd    178
ab1-var-1.log        q1   cold   8504 n  30 median    6514 mean    6523 min   6189 sd    184
ab1-var-1.log        q6   cold   5250 n  30 median    4978 mean    4984 min   4759 sd    110
ab1-var-2.log        q1   cold   8772 n  30 median    6416 mean    6425 min   6137 sd    172
ab1-var-2.log        q6   cold   5149 n  30 median    4914 mean    4953 min   4702 sd    186
ab1-var-3.log        q1   cold   9142 n  30 median    6498 mean    6487 min   6133 sd    144
ab1-var-3.log        q6   cold   5719 n  30 median    4977 mean    4956 min   4651 sd    145
```

Round 2 — base vs V3 vs V3ns (verbatim):
```
ab2-base-1.log       q1   cold   8954 n  30 median    6342 mean    6366 min   6089 sd    172
ab2-base-1.log       q6   cold   5129 n  30 median    4698 mean    4691 min   4341 sd    126
ab2-base-2.log       q1   cold   8319 n  30 median    6212 mean    6248 min   5975 sd    149
ab2-base-2.log       q6   cold   4695 n  30 median    4632 mean    4641 min   4392 sd    165
ab2-base-3.log       q1   cold   8188 n  30 median    5870 mean    5858 min   5598 sd    144
ab2-base-3.log       q6   cold   4639 n  30 median    4524 mean    4524 min   4306 sd    125
ab2-v3-1.log         q1   cold   9298 n  30 median    6169 mean    6186 min   5938 sd    158
ab2-v3-1.log         q6   cold   4986 n  30 median    4746 mean    4745 min   4475 sd    128
ab2-v3-2.log         q1   cold   8333 n  30 median    6064 mean    6065 min   5739 sd    200
ab2-v3-2.log         q6   cold   4627 n  30 median    4638 mean    4637 min   4318 sd    165
ab2-v3-3.log         q1   cold   8666 n  30 median    6189 mean    6182 min   5892 sd    154
ab2-v3-3.log         q6   cold   5124 n  30 median    4712 mean    4702 min   4439 sd    132
ab2-v3ns-1.log       q1   cold   7993 n  30 median    5986 mean    5963 min   5587 sd    168
ab2-v3ns-1.log       q6   cold   4946 n  30 median    4550 mean    4558 min   4329 sd    132
ab2-v3ns-2.log       q1   cold   8168 n  30 median    5854 mean    5874 min   5579 sd    181
ab2-v3ns-2.log       q6   cold   4639 n  30 median    4513 mean    4513 min   4175 sd    131
ab2-v3ns-3.log       q1   cold   8467 n  30 median    6142 mean    6160 min   5920 sd    147
ab2-v3ns-3.log       q6   cold   4739 n  30 median    4550 mean    4564 min   4360 sd    122
```

Mean of per-JVM medians (ms):

| | Q1 | Q6 |
|---|---|---|
| round 1 base | 6555 | 4865 |
| round 1 V1 | 6476 (−1.2%) | 4956 (+1.9%) |
| round 2 base | 6141 | 4618 |
| round 2 V3 | 6141 (0%) | 4699 (+1.8%) |
| round 2 V3ns | 5994 (−2.4%) | 4538 (−1.7%) |

Per-JVM spread of medians is ±4% (base round 2 Q1: 5870–6342), so none of these is a signal.
Plain `main` without `-PnarrowVarTypes` not run (time spent on the regression below instead).
All bench runs passed the pinned-result check.

### JFR (15 hot reps, one query per JVM; samples under `ScanCursor.tryAdvance`, % of query)

Q1 (base 5477 samples, V3 5791):

| phase | base | V3 |
|---|---|---|
| event merge (EntityMerge/resolver/PQ) | 26.6% | 27.4% |
| group-by key mapping (GroupMapper/DistinctRelationMap) | 19.9% | 25.3% ¹ |
| generated expression fns (predicate + projections) | 15.7% | 13.1% |
| agg `sum` | 8.3% | 6.1% |
| page load | 6.1% | 5.4% |
| per-batch expr setup (param-types, var-types, emit memo) | 3.6% | 3.8% |
| `RelationReader.select` (composition) | 1.8% | 0.7% |
| `VectorMask.open` + IntStream (sel building) | ~1.1% | ~1.0% |

Q6 (base 3432, V3 3413):

| phase | base | V3 |
|---|---|---|
| event merge | 42.0% | 42.0% |
| generated expression fns | 15.1% | 14.8% |
| page load | 9.8% | 9.8% |
| per-batch expr setup | 9.6% | 9.4% |
| `VectorMask.open` | 3.0% | 2.6% |
| `RelationReader.select` | 1.5% | 1.3% |

¹ not the sel: both variants allocate a `ByteBuffer` (via `NativeMemorySegmentImpl.makeByteBuffer`) per utf8 key hash/compare, plus `MutableMemoryHashTrie.intToByteArray` per insert; the shift between variants is where C2 did/didn't scalar-replace it.
In the V1 combined Q1+Q6 profile GroupMapper was 1201→1211 samples, i.e. flat.

Top-self frames moved from `ValueReader$ForVector.readInt/readDouble` and `IndirectVector$valueReader$1.*` (base) to `FixedWidthVector.getInt0/getDouble0` (V3) — the remaining cost in the generated fns is the memory access itself (`MemorySessionImpl.checkValidStateRaw`, bounds checks), not dispatch.

### Per-value interface dispatches (non-null f64 var read in a generated fn)

- base (`IndirectVector` → `ValueReader`): `setPos` (IndirectVector's reader) → `VectorIndirection.get` → `getIndex` → inner `setPos`; `readDouble` (delegating reader) → inner `readDouble` → `vec.getDouble` from `ForVector` — **7**, the last at a JVM-wide shared site (megamorphic).
- V1: `setPos`, `aget`, `readDouble`, `vec.getDouble` (still the shared `ForVector` site) — **3**.
- V3: `aget`, `vec.getDouble(i)` at a call site private to the generated class (monomorphic → inlined) — **1**.
- nullable adds `isNull`: base 3 more, V3 1 more.
- `sum` per row: base `sumRow` (IndirectVector lambda) → `sel[idx]` (2) → inner `sumRow`; V1 the inner `sumRow` only.
- group-by per row per key col: base `IndirectVector.hashCode`/`getPointer` → `sel` (2) → inner; V1 direct.

## Data structures

Matched:
- **`IntArray` sel on the vector, shared by identity across the columns that came from one source.**
  The scan already produces this: `BitemporalConsumer` selects the `put` leg once (`LegReader.select` → `StructVector.select`), and every child `IndirectVector` shares that `Selection`'s array.
  Identity is the group: "these columns share a selection" needs no new type — `IdentityHashMap` in `select` keeps it through composition.
- **Specialising the emitted loop on which vars are selected** (memo key), rather than a per-row null branch.
- **`direct` + `selIdxs` as defaulted members**: every existing consumer stays correct reading the view; awareness is opt-in per consumer.

Didn't:
- A relation-level `(groups, sel)` type (`IndirectRelation`, predecessor spike): same composition count as the identity-dedup above, but adds a `RelationReader` impl whose `vectors` must manufacture views — and consumers would have to downcast to benefit.
  Identity on the vectors gives the same grouping for free and survives `ProjectCursor`/`RelationReader.from` re-packing, which a relation type doesn't.
- Bitmasks: not tried as a carrier.
  On this path the mask is already an intermediate (`VectorMask.open` → `IntStream` → `IntArray`), ~1–3%; a sel-aware selection spec could emit the composed `IntArray` straight from the generated loop and drop both (see sub-goals).
  A mask as the *carrier* would make every downstream loop scan all rows; Q6 keeps ~2% of rows, so sel wins there.
- `Slice` indirections are not unwrapped (`selIdxs` null) — they'd need materialising or a second index form in codegen; not on the Q1/Q6 path.

## Blast radius

- Contract change is additive: `VectorReader.direct`, `VectorReader.selIdxs` (defaults: `this`, `null`).
- Must override (else wrong answers): any `VectorReader by inner` delegator — `RenamedVector` (`direct` must stay renamed), `DenseUnionVector.LegVector` (delegates to a `LegReader`, so `direct` would leak the reader).
- Sel-aware consumers touched: `expression.clj` `:variable` codegen (+ `->expression-projection-spec` computing `:sel-vars`), `agg/Sum`, `agg/Count`, `agg/GroupMapper.Mapper` (+ `DistinctRelationMap.storeFullBuildRel` made public), `RelationReader.select`.
- Unchanged and still correct through the views: joins (`BuildSide`, `ProbeSide`, `ComparatorFactory`), distinct, sort, window, other aggs (min/max, array-agg, variance…), `rowCopier` users, `PatchGapsCursor`, flight-sql.
  Each is a candidate for the same treatment; none required.
- Scan untouched — `BitemporalConsumer` already shares the sel array; `openSlice` of an `IndirectVector` keeps its `Selection` object.

## Invariant broken (found by `xtdb.tpch-test/test-001-ra` Q22, fixed in the last commit)

- `RenamedVector` delegated `field` to its inner (old name).
  Harmless while a rename was always outermost (its own `select`/`openSlice`/`openDirectSlice` re-wrapped).
  `select` composing on `v.direct` puts a `RenamedVector` *inside* an `IndirectVector`, whose `field` forwards to the inner → `openDirectSlice` materialised `_id` instead of `c_custkey`.
  Fix: `RenamedVector.field` uses its own name. Latent bug on `main` (`IndirectVector(RenamedVector(...), sel)` built any other way would hit it); worth landing independently.
- Arrow ownership: unchanged — no new owned buffers; composed sels are heap `IntArray`s; `direct` never transfers ownership. No leaks reported by the operator/tpch tests.
- Nested indirection: `IndirectVector.direct` only unwraps when the inner has no `selIdxs`, so `Indirect(Indirect(...))` reads through the view (correct, slow path). The scan with an iid predicate (`MultiIidSelector`) produces this shape.

## Tests

- `:test --tests 'xtdb.operator*' 'xtdb.expression*' 'xtdb.tpch*' 'xtdb.sql*'`: 755 tests, 1 failure (Q22, above) before the fix, 0 after.
- Full `:test :xtdb-core:test` on V3 + fix: root 1876 tests, 1 failure — `xtdb.authn-test` initializationError, from the local no-Docker patch below, not the change; `:xtdb-core:test` 601/601.
  Not run: `integration-test`, `property-test` (scan/indexing untouched).
- Env note: `:compileTestClojure` fails here without Docker — `xtdb.authn-test` constructs a `KeycloakContainer` in a `defonce` at load. Patched locally (not committed) to run tests.

## Suggested sub-goals

If anyone wants this anyway — the numbers say it's not where Q1/Q6's time goes.

1. **advance/fix**: `RenamedVector.field` reports its own name. Independent; latent on main.
2. **tidy**: `VectorReader.direct`/`selIdxs` (defaulted), overridden on `IndirectVector`, `RenamedVector`, `LegVector`. No behaviour change. Depends on 1.
3. **advance (perf)**: `RelationReader.select` composes once per distinct sel array. Depends on 2. This is #6285's literal title; it's ~1% of Q1 and the cheapest real win here.
4. **advance (perf)**: codegen `:variable` reads `direct` at `sel[i]`, specialised via `:sel-vars` in the emit memo key. Depends on 2.
5. **advance (perf)**: codegen reads primitives via `vec.getX(i)` rather than `ValueReader` (independent of 2–4; V3ns shows it alone is as good as everything combined).
6. **advance (perf)**: `Sum`/`Count`/`GroupMapper` read direct at sel. Depends on 2.
7. Better targets surfaced by the profile (not this avenue): utf8 group-key hash/compare allocating a `ByteBuffer` per call (20–25% of Q1); per-batch `emit-projection` memo lookup / param-types (9.5% of Q6); `MutableMemoryHashTrie.intToByteArray` per insert.

## Questions

- Is the bar wall-clock on Q1/Q6, or interface-call count per value as a structural property? (Blocks whether 4–6 are worth landing at all.)
- Should `Slice` be unwrapped too (materialise to `IntArray`, or a `(start, len)` form in codegen)? Blocks extending 4 beyond the scan path.
- Is identity-sharing of sel arrays an invariant we want to *promise* (e.g. `RelationReader.from` docs), given 3 and 6 depend on it for their win but not for correctness?

## Assumptions / risks

- Assumed `emit-projection`'s memo key growing by `:sel-vars` doesn't blow the LRU — it adds at most one variant per (expr, types) in practice.
- `GroupMapper` direct path relies on `DistinctRelationMap` only reading the build rel at the indices it's given — true today for `!storeFullBuildRel`.
- Bench noise (±4% across JVMs, ~6% drift over an hour) bounds what a dart can see; anything under ~3% needs more JVMs.

## Dead-ends

- A new relation type (`SelRelation`/`IndirectRelation`) — dropped before building: sel-array identity on the existing vectors gives the same grouping with no new `RelationReader` impl.
- Expecting the per-value dispatch removal to show on wall-clock: the JFR shows C2 already inlined most of it; the generated fns' remaining self time is the loads.
- Bisecting the Q22 failure via `JAVA_TOOL_OPTIONS` sysprops broke the Clojure test-engine discovery; found the cause by reading instead.

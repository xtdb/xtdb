# #6285 avenue B — bitmask (`MaskedRelation`)

Branch `dart/6285-bitmask` = xtdb/xtdb `main` (418e8b4) + harness (658867b) + dart commits.
Never for landing.

## TL;DR

- Works end to end for Q1/Q6, and results verify.
  Scan → column predicates → project → group-by mapper / sum / avg / count all iterate set bits over the direct vectors.
  Nothing on the Q1/Q6 path goes through `IndirectVector` any more (JFR: 0 samples).
- **Win: Q1 −3.2%, Q6 −5.8%** hot (mean of per-JVM medians, 3 interleaved JVM pairs, and every pair agrees).
- **The ceiling is low.**
  Selection machinery (IntStream/VectorMask/`int[]` composition) is only **~3%** of baseline query samples.
  Event merge/resolve is ~36–42%, and expression eval plus group-by is ~40%.
  The bitmask removes the 3%, and makes the reads in eval/project cheaper by dropping the per-read `sel[idx]` hop.
- JVM-to-JVM noise is ±5–10% (round 1 was accidentally an A/A run, see below), so you need ≥3 interleaved pairs to see effects of this size at all.

## Numbers

SF1, TPC-H RA, 50 hot reps per JVM, first 20 dropped, `-PnarrowVarTypes` on both sides.
Runs were interleaved: base2-0, mask2-0, base2-1, mask2-1, base2-2, mask2-2.

Round 2 (`runs-B/round2.txt`), `runs.py --drop 20`, verbatim:

```
base2-0.log          q1   cold  10865 n  30 median    7835 mean    7850 min   7506 sd    189
base2-0.log          q6   cold   6221 n  30 median    5876 mean    5886 min   5589 sd    162
base2-1.log          q1   cold  10646 n  30 median    7845 mean    7905 min   7552 sd    223
base2-1.log          q6   cold   6480 n  30 median    5892 mean    5900 min   5476 sd    224
base2-2.log          q1   cold  11924 n  30 median    8063 mean    8092 min   7779 sd    196
base2-2.log          q6   cold   6804 n  30 median    6082 mean    6082 min   5536 sd    238
mask2-0.log          q1   cold  10219 n  30 median    7723 mean    7741 min   7211 sd    228
mask2-0.log          q6   cold   5444 n  30 median    5684 mean    5698 min   5378 sd    197
mask2-1.log          q1   cold  10358 n  30 median    7632 mean    7637 min   7323 sd    223
mask2-1.log          q6   cold   5695 n  30 median    5562 mean    5569 min   5277 sd    212
mask2-2.log          q1   cold  10924 n  30 median    7618 mean    7616 min   7103 sd    214
mask2-2.log          q6   cold   6052 n  30 median    5575 mean    5615 min   5298 sd    195
```

- Q1 median of medians: base 7845 → mask 7632.
  Mean of medians: 7914 → 7658 (−3.2%).
  Min: 7506 → 7103.
- Q6 median of medians: base 5892 → mask 5575.
  Mean of medians: 5950 → 5607 (−5.8%).
  Min: 5476 → 5277.
- The sparse case (Q6, ~1.9% of rows survive) gains more than the dense one (Q1, ~98%), as expected.
  - Q6's later predicates and its projection only visit surviving bits, and the word skip makes empty words free.
  - On Q1, word-wise iteration costs about what the `sel[i]` load it replaces did. The gain is the dropped allocations and per-column composition, not cheaper per-row iteration.

Round 1 (`runs-B/round1-aa.txt`) was an **A/A run by accident**.
The mask path silently never engaged, because the `put` leg is a `DenseUnionVector.LegVector`, not the `LegReader` I tested for.
So `aa-mask-*` and `aa-maskNoAgg-*` are the baseline plus a few failed `instanceof` checks.
Per-JVM Q1 medians across the 9 A/A JVMs ranged 7566–8453 (≈ ±6%), and Q6 5688–6257.
`main-0` (no narrowVarTypes) was cut off at ~29 reps by a session time limit: Q1 8154 and Q6 5942 on 8–9 post-drop reps, so read it as reference only.

### JFR (query samples only, i.e. under `ScanCursor.tryAdvance`; `analyse_stream.py`, a streaming variant because `analyse.py` OOMs on 55MB recordings)

| category | base | mask-v2 | Δ |
|---|---|---|---|
| event merge / resolve | 8001 | 8873 | +872 (JIT/inlining noise, untouched code: top-self frames reshuffle between JVMs) |
| expression eval (preds + projections) | 5455 | 4050 | **−1405** |
| group-by (mapper + aggs) | 3883 | 4039 | +156 |
| param-types/zone setup | 1661 | 1476 | −185 |
| project | 1467 | 1129 | −338 |
| page load | 1382 | 1249 | −133 |
| selection (IntStream/VectorMask/`int[]`) | 673 | 113 | **−560** |
| total | 22542 | 20937 | −7% (−18% excluding merge) |

- mask-v1 (BitSet + `nextSetBit` per row, linear `isDirect`) cut eval and selection the same way, but group-by went **up** (+521).
  - `MaskedRelation.isDirect` alone was 1.6%: a linear name scan, called per batch, per spec.
  - `nextSetBit` per row costs about what an array-indexed selection does.
  - v2 fixed both: `long[]` words, a word-wise `numberOfTrailingZeros` / `w & (w-1)` loop, and name maps.
- Remaining mask-specific overhead in v2 is per batch, not per row: `MaskedRelation` construction (name maps, lazy `directRel`, `bitCount`) shows ~1.5% of samples.
  **Batches are small** (one per leaf page), so per-batch object churn matters as much as the per-row work.

## What the code does (`git diff bench-base`)

- `xtdb.arrow.MaskedRelation(direct, physicalCount, words: LongArray, dense)` is a `RelationReader`.
  - `direct` are the unselected vectors, `words` is one bitmask over their rows, and `dense` are vectors already in logical space (one row per set bit).
  - Mask-aware consumers call `MaskedRelation.forEachSetBit(words) { p -> … }`, an inline word loop. They read `direct` at `p` and `dense` at a running `i`, in lockstep.
  - Everyone else uses the plain `RelationReader` view: `selection` (the mask resolved once to an `int[]`, lazily) is shared by every direct column via `v.select(sel)`.
    So non-mask-aware operators still get "compose once".
    `select(idxs)` composes once too.
- Scan (`BitemporalConsumer.RelBuilder.build`):
  - When the content selection is strictly increasing and the leaf has a `put` leg, it builds the mask **in the put struct's own index space**, via the union's leg offsets, which are monotonic within one leg.
  - It hands the put struct's children over as `direct`.
  - `_iid`, which lives in leaf-row/op space rather than put space, and the temporal columns (written per emitted row) go in `dense`.
  - Otherwise it falls back to the old path.
- Column predicates: `MaskRefiner.refine` copies the words (`aclone`) and runs a codegen'd loop that clears the bits where the predicate isn't true.
  - It is used in `ScanCursor`'s colPred fold and in `SelectCursor`.
  - So there is no bool vector, no `VectorMask`, no `IntStream`, and no `int[]` (#6163's triple materialisation collapses to one in-place word write).
- `ProjectCursor`:
  - Identity/rename specs stay `direct`.
  - Expression specs implement `MaskedProjection.projectMasked`: a codegen'd loop over the set bits that reads the direct vectors at `p` and appends `dense` output.
  - The result is still a `MaskedRelation`, carrying the same words.
- `GroupMapper.Mapper`, `Sum`, `Count` (and `Average`, via Sum and Count) iterate set bits when the column is direct and the incoming `VectorMask` is `ALL`.
- Codegen (`expression.clj`): `emit-projection` additionally yields `:!masked-projection-fn` and `:!mask-refine-fn`.
  Both use the same `continue`/`batch-bindings`, with the `dotimes` replaced by a word loop over `idx-sym`.
  Expressions only qualify when **every** variable is direct; otherwise it falls back.

## Tests

- `./gradlew :test --tests 'xtdb.operator*' --tests 'xtdb.expression*' --tests 'xtdb.tpch_test*' --tests 'xtdb.sql_test*'` on mask-v2 ran **561 tests: 0 failures, 0 errors**.
  The masked scan engaged in that JVM (stderr marker).
- Caveat: this container has no Docker, and root `compileTestClojure` AOT fails on testcontainers namespaces.
  I moved 8 namespaces aside for the run and then restored them: authn, kafka, pgwire_protocol, oidc_integration, aws/s3, azure, gcp, pgwire/authn.
  Not run: the full `./gradlew test`, property tests, or `:xtdb-core` Clojure tests beyond `xtdb.operator*`/`xtdb.expression*` (the Kotlin ones, which passed).
- So the green run says the fallback view and the masked paths agree on what the tests cover.
  It says nothing about leaks under failure, or about the iid-selected/polygon scan paths with masks (they fall back).

## Data structures

**Matched the problem:**
- One `long[]` per relation, shared by every column.
  The AND is implicit: a predicate only visits set bits and clears its failures in place on a copy.
  Composition is O(words), not O(rows × columns).
- Word-wise iteration (`numberOfTrailingZeros`, `w & (w-1)`).
  Empty words cost one compare, which is what makes the sparse case cheap.
- "Direct group + dense group, iterated in lockstep."
  Projections need to append computed columns, and lockstep keeps the computed columns dense without forcing the input to resolve.
- The put struct's index space for the mask.
  It is a property of the leaf format (one leg's offsets are monotonic) that makes "direct" truly direct.

**Didn't:**
- `java.util.BitSet` + `nextSetBit` per row.
  Per-row cost is about equal to a selection load (bounds/`wordsInUse` checks per call), so there is no gain on dense data.
- A mask in leaf-row (op-union) space.
  The content columns live under the `put` leg, so every read would still hop through union offsets (`LegVector`), and `sumInto` isn't implemented on `LegReader`.
- The `RelationReader` contract itself.
  `rowCount` plus random-access `getX(idx)` is logical-row-indexed, so a mask can only be honoured by consumers that opt in; everyone else must resolve to `int[]`.
  The fallback (resolve once, share) is fine, but it means the bitmask is a *side channel* on top of `RelationReader`, not a replacement for it.

## Operators that couldn't use it (and resolve to the shared `int[]`)

- Anything needing random access, reordering or duplication: joins (build and probe), sort/order-by, distinct, window, unnest, top/limit (`select(start, len)`), set ops.
- Anything that takes columns as `vectors` wholesale: `RelationReader.openSlice`, `rowCopier`, `toArrowStream`, `openArrowRecordBatch`, pgwire/flight result serialisation.
- Expression predicates/projections that reference a `dense` column (e.g. `_iid`, temporal columns, or a column computed by an upstream project), and filtered aggregates (incoming `VectorMask` ≠ `ALL`).
- All other aggregates (min/max, var/stddev, array_agg, percentile …): only `Sum`/`Count` (and therefore `Avg`) and `GroupMapper.Mapper` are mask-aware.
- Scan when temporal resolution emits a leaf row more than once, or out of order (polygon resolver / valid-time ranges), when the leaf has no `put` leg, or when it was iid-selected.

## Suggested sub-goals (if this avenue is pursued)

Each is one tidy (structure only) or one advance (behaviour).

1. **tidy** — `RowMask` value type (`long[]` + physical count + `forEachSetBit`, `cardinality`, `toSelection`), with no consumers.
2. **tidy** — `MaskedRelation : RelationReader` whose fallback view resolves the mask once and shares the `int[]` (this is already a net "compose once" with zero consumers).
   Depends on 1.
3. **advance** — scan emits `MaskedRelation`, in put-struct index space, when the content selection is strictly increasing.
   Depends on 2, and on `LegVector.getOffset` being exposed (a tidy).
4. **advance** — `MaskRefiner` for expression predicates, used in `ScanCursor` and `SelectCursor`.
   Codegen variant of `emit-projection` with a word loop and in-place clears.
   Depends on 3.
   This alone delivers most of the selection-side win (#6163's triple materialisation).
5. **advance** — `MaskedProjection` in `ProjectCursor` (identity stays direct, computed columns dense).
   Depends on 4's codegen.
6. **advance** — mask-aware `GroupMapper`/`Sum`/`Count` (then the other aggs one by one).
   Depends on 5.
7. **tidy** — cut per-batch overhead.
   - Don't rebuild name maps or `directRel` per derived `MaskedRelation`: share an immutable "direct group" object between scan, refine and project.
   - Memoise the `every? isDirect` qualification per (spec, input-shape).

Also consider one sub-goal *before* any of these: make scan batches bigger, so the per-batch overheads don't eat the per-row savings.

## Questions

- Is a ~3–6% hot win on Q1/Q6 worth a parallel "mask-aware" consumer path in project/agg/codegen?
  The profile says the big fish is event merge/resolve (~40%: `PriorityQueue`, `EventRowPointer` comparators, `PropertyReference1.invoke` in `iidComparator`).
  Blocks: whether to pursue sub-goals 5–6 at all, versus stopping at 2–4.
- Should the bitmask replace `VectorMask` in group-by's filtered aggregates (`VectorMask` is already a bitset, but in logical space)?
  Blocks: the shape of the agg API (two masks, or one in physical space).
- How big are scan batches meant to be (rows per leaf page here)?
  Per-batch costs are visible in the profile.
  Blocks: whether sub-goal 7 matters.

## Assumptions, risks, invariants broken

- **Index-space assumption:** the scan's content selection is strictly increasing, and maps through monotonic put-leg offsets.
  It is checked per batch (`isStrictlyIncreasing`), and falls back otherwise.
  It also assumes every resolved row *is* a put; the old path assumed the same.
- **`RelationReader` contract:** `vectors`/`vectorForOrNull` now return lazily-built `IndirectVector` views that are cached per relation.
  Closing a view closes the direct vector (the same as before), but the relation's `close()` closes the direct vectors too.
  So a consumer that closes both double-closes; the old `FromCols` of `IndirectVector`s had the same hazard.
- **Memory ownership:** `ProjectCursor.tryMasked` closes its computed dense vectors after `accept`, the same as the existing path.
  `MaskedRelation.openSlice` slices whole put-struct children (all physical rows), the same as `IndirectVector.openSlice` did.
  No leak checks were run beyond the operator tests below.
- **The `words` immutability** convention is unenforced: refine `aclone`s, everyone else must not write.
- **Error mapping:** `projectMasked` skips the `ArithmeticException`/`ClassCastException` → anomaly mapping the normal path does.
- **Checked math in codegen:** `(dec w)` overflows at `Long.MIN_VALUE`, so it has to be `unchecked-dec`.
  This hit me once.
- **`GroupMapper` masked path** hands `directRel` to `DistinctRelationMap.buildFromRelation`.
  If `storeFullBuildRel` is true, the stored build rel would only carry the direct columns; it is not checked.
- **Nested indirection:** direct vectors are put-struct children.
  If the leaf was iid-selected (`maybeSelect`), the `op` vector is an `IndirectVector`, the `LegVector` check fails and the old path is used.
  It is untested whether that's always the case.

## Dead-ends

- Round 1: `putLeg is LegReader` is never true, because the leg is a `LegVector`.
  I only noticed via JFR (zero `MaskedRelation` frames).
  The harness should probably assert that the variant path engaged, e.g. a counter in the bench output.
- `BitSet.nextSetBit` per row: no faster than `int[]` indirection on dense data, and group-by got slower.
  Replaced by word loops.
- A mask over leaf rows (op space) with the put children read through `LegVector`: it fails at `sumInto` (unsupported on `LegReader`), and it would keep the offset hop anyway.
- `analyse.py`: it OOM-kills on 55MB JFRs (`jfr print --json` slurped whole).
  `analyse_stream.py` streams the text output instead.

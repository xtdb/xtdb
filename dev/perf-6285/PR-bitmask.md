Part of #6285 — avenue B (bitmasks, #4964): a dart probe, not for merging.

Scan filters now compose one `long[]` bitmask over the direct vectors, and project and group-by iterate its set bits, giving Q1 −3.2% and Q6 −5.8% hot at SF1.

- **The selection machinery is ~3% of baseline Q1/Q6 time, which caps what any selection avenue can win**
  JFR, query samples only: IntStream/VectorMask/`int[]` composition is 673 of 22542. Event merge/resolve is ~36–42% (`PriorityQueue`, the `EventRowPointer` comparators, `PropertyReference1.invoke` in `iidComparator`), and expression eval plus group-by is ~40%.

- **The bitmask removes that 3%, and makes eval/project reads cheaper by dropping the `sel[idx]` hop**
  Expression eval 5455 → 4050 samples, selection 673 → 113, project 1467 → 1129. Total 22542 → 20937 (−7%, or −18% excluding merge, whose code this doesn't touch).

- **The wall-clock win is real but sits inside JVM-to-JVM noise, so it needs several interleaved pairs to show**
  All 3 interleaved base/mask pairs favour the mask. An accidental A/A round (below) put per-JVM Q1 medians anywhere in 7566–8453 for identical code.

- **The bitmask can only ever be a side channel on `RelationReader`, never a replacement for it**
  `rowCount` plus random-access `getX(idx)` is logical-row-indexed. So a consumer only benefits by opting in, and everything else resolves the mask to an `int[]`, once per relation, shared across columns.

## What changes

The scan no longer hands downstream a relation of per-column `IndirectVector`s, each carrying its own composed `int[]`. When it can, it hands over a `MaskedRelation` instead; otherwise it falls back to the old path.

- **`MaskedRelation` is three things: the direct vectors, one `long[]` mask over their rows, and dense vectors with one row per set bit**
  Mask-aware consumers iterate `forEachSetBit(words)`, an inline word loop over `numberOfTrailingZeros` / `w & (w-1)`. They read direct vectors at the physical index and dense vectors at a running logical index, in lockstep.

- **Every other consumer sees an ordinary `RelationReader` whose direct columns share one lazily-resolved `int[]`**
  So operators that know nothing of masks still compose the selection once rather than once per column, and `select(idxs)` composes once too.

- **The mask lives in the put struct's own index space, not the leaf's row space**
  Content columns sit under the `op` union's `put` leg, whose offsets increase monotonically within the leg. So a leaf-row selection maps to a put-struct mask, and the put struct's children become truly direct vectors with no union-offset hop.

  - **`_iid` and the temporal columns go in the dense group**
    `_iid` lives in leaf-row space, and the temporal columns are written once per emitted row.

  - **The scan takes the masked path only when the content selection is strictly increasing and the leaf has a `put` leg**
    Otherwise it falls back. That covers the polygon resolver emitting a row twice, out-of-order emission, erase-only leaves, and iid-selected leaves.

- **A column predicate clears mask bits in place instead of materialising three times**
  `MaskRefiner.refine` copies the words and runs a generated loop that visits only set bits, clearing those where the predicate isn't true. That replaces #6163's bool vector → `VectorMask` → `int[]`. It is used in `ScanCursor`'s column-predicate fold and in `SelectCursor`.

## Consequences

- **Measurements**
  SF1, TPC-H RA, 50 hot reps per JVM with the first 20 dropped, `-PnarrowVarTypes` on both sides, runs interleaved base, mask, base, mask, base, mask.

  | | base, mean of per-JVM medians | mask | change |
  |---|---|---|---|
  | Q1 (~98% of rows survive) | 7914 ms | 7658 ms | −3.2% |
  | Q6 (~1.9% survive) | 5950 ms | 5607 ms | −5.8% |

  - **The sparse query gains more because empty mask words cost one comparison and later predicates only visit survivors**
    On the dense one, a word loop costs about what the `sel[i]` load it replaces did. The gain there is the dropped per-column composition and allocation.

  - **The raw logs, `runs.py` output and JFR breakdowns are in `dev/perf-6285/runs-B/`, and the full report is `dev/perf-6285/REPORT-bitmask.md`**

- **Batches are one leaf page, so per-batch overhead competes with the per-row savings**
  Building a `MaskedRelation` per batch (name maps, the lazy `directRel`, `bitCount`) shows at ~1.5% of samples. A linear `isDirect` scan in the first version was 1.6% alone.

- **Mask words are immutable only by convention**
  `refine` clones before writing. Nothing stops another consumer writing into a mask that a sibling relation shares.

- **`projectMasked` skips the anomaly mapping the normal projection path does**
  `ArithmeticException`, `ClassCastException` and friends propagate raw from a masked projection.

- **The generated loop must use `unchecked-dec`**
  `(dec w)` overflows when the top bit is set (`Long.MIN_VALUE`), under XTDB's checked maths.

- **Cached views share their direct vector with the relation**
  Closing a view closes it, and so does the relation's `close()`. That is the same double-close hazard the old relation of `IndirectVector`s had.

- **The masked `GroupMapper` path hands `DistinctRelationMap` only the direct columns**
  Where `storeFullBuildRel` is true, the stored build relation would lack the dense columns. Unchecked.

- **Tests: 561 operator, expression, TPC-H and SQL tests pass, with the masked scan engaged**
  The container had no Docker, so 8 testcontainers namespaces were moved aside during AOT and then restored. The full `./gradlew test`, property tests and leak checks were not run.

## Out of scope

- **Landing this branch**
  It is a dart: no tests of its own, no review, and debug scaffolding (a stderr marker, `xtdb.masked` / `xtdb.maskedAgg` system properties, `-PxtProps`). The landed shape is the sub-goals below, each one tidy (structure only) or one advance (behaviour).

  1. **tidy — a `RowMask` value type with no consumers**
     `long[]` plus physical count, `forEachSetBit`, `cardinality`, `toSelection`.
  2. **tidy — `MaskedRelation` with its resolve-once fallback view, which is already "compose once" with zero mask-aware consumers**
     Depends on 1.
  3. **advance — the scan emits masks in put-struct index space**
     Depends on 2, and on exposing `LegVector.getOffset`, which is a tidy of its own.
  4. **advance — mask-refining predicates in `ScanCursor` and `SelectCursor`, via the generated word loop**
     Depends on 3. This alone delivers most of the selection-side win.
  5. **advance — masked projection in `ProjectCursor`**
     Depends on 4's codegen.
  6. **advance — a mask-aware group mapper and `Sum`/`Count`, then the other aggregates one by one**
     Depends on 5.
  7. **tidy — cut per-batch overhead**
     Share one immutable direct-vector group between the relations a batch derives, rather than rebuilding name maps and `directRel`, and memoise the per-spec "every variable is direct" check.

- **Operators that need random access, reordering or duplication**
  Joins, sort, distinct, window, unnest, top/limit and set operations still resolve to the shared `int[]`, as does anything taking the vectors wholesale: `openSlice`, row copiers, Arrow/pgwire/Flight serialisation.

- **Aggregates other than sum, count and avg, and filtered aggregates**
  Only `Sum` and `Count` (so `Average`) and the group mapper are mask-aware. A filtered aggregate's `VectorMask` is in logical space, so the masked path doesn't apply to it.

- **Expressions reading a dense column**
  `_iid`, the temporal columns, and any column computed by an upstream projection force the fallback.

## Alternative approaches

All were tried and abandoned on 2026-10-24 unless marked otherwise.

- **`java.util.BitSet` with `nextSetBit` per row — tried, abandoned**
  Per-row cost about equals an `int[]` selection load, because each call re-checks bounds and `wordsInUse`. With it, group-by got slower (+521 samples) and no wall-clock gain showed.

- **A mask over leaf rows, reading content columns through the `put` leg — tried, abandoned**
  Every read still hops through the union offsets, and `sumInto` is unsupported on `DenseUnionVector.LegReader`, so `Sum` threw on the first query.

- **A predecessor `IndirectRelation` spike (one selection per column group) — reference only, not built on**
  It composes once per group but keeps random access through an `int[]`, so predicates still materialise the bool vector → `VectorMask` → `int[]` chain this branch removes.

## Open questions

Nothing here can be settled on a PR, so each needs a card on #6285; none has been raised.

- **Q1. Is a 3–6% hot win on Q1/Q6 worth a parallel mask-aware path through project, aggregation and codegen?**
  Event merge/resolve is ~40% of the same profile. Settled by a decision on whether to stop at sub-goals 2–4, which carry most of the win with the least parallel code, or carry on to 5–6.

- **Q2. Should the physical-space mask replace `VectorMask` in group-by's filtered aggregates?**
  Settled by deciding the aggregate API shape: two masks (physical and logical), or one.

- **Q3. How many rows is a scan batch meant to carry?**
  Per-batch overhead is visible at leaf-page batch size. Settled by a measurement of rows per batch on SF1 lineitem, which decides whether sub-goal 7 matters or larger batches come first.

- **Q4. Do the scan's two index-space assumptions hold everywhere the masked path engages?**
  Every resolved row is a put, and the leaf has not been iid-selected. Both are unverified beyond the fallback checks. Settled by tests covering erase/delete-bearing leaves and `MultiIidSelector` scans with the masked path forced on.

- **Q5. Does the masked path leak under failure?**
  No leak checks were run. Settled by the operator suite with the debug allocator, plus a forced exception mid-`projectMasked`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01V3tHPJ3dnPFAazRUtG9PbJ

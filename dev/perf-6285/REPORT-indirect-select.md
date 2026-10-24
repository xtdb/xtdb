# #6285 avenue A: IndirectRelation applied to select/project (dart report)

Branch `dart/6285-indirect-select` = xtdb/xtdb main 418e8b4 + harness (c04f652) + spike (31f1699) + this dart's commits.
Code is evidence, not for landing.
I couldn't read #6285/#4964/#6163: this session only reaches jarohen/xtdb, so I worked from the brief alone.

## What was built

- `IndirectRelation(inner: RelationReader, sel: IntArray?)`: **one selection for the whole relation**, replacing the spike's per-group `sel`.
  - `select(idxs)` composes one array; column views are `IndirectVector(innerVec, sel)`, deliberately *not* flattened via `VectorReader.select`, which composes an array per column (and per `vectorFor` call).
  - Two index spaces (content columns index leaf rows, temporal columns are dense) are folded at build time: content columns become `IndirectVector(v, contentSel)` inside `inner`, and `sel = null`.
- `SelectionSpec.refine(al, inner, sel, schema, params): IntArray` (default = old select + compose) and `SelectionSpec.selectBy(rel, spec, …)`.
  - The expression engine's override generates a loop: `idx = sel[i]`, evaluate the predicate on `inner` at `idx`, keep the passing `idx`.
    It writes no mask vector, uses no `IntStream`, does no composition, and reads `inner`'s direct vectors with no `IndirectVector` value-reader hop.
  - The scan's column predicates (`ScanCursor` fold) and `SelectCursor` both go through `selectBy`, so successive predicates shrink one array.
- `ProjectionSpec.projectSel(al, inner, sel, …)`: the expression engine evaluates `inner[sel[i]]` and writes dense output (sparse mode).
  `ProjectCursor` has `-Dxtdb.projectMode=sparse|dense|off`; dense evaluates every inner row and emits `IndirectRelation(outCols, sel)`.
- **Put-leg composition** in `BitemporalConsumer.build`: when every content column is a child of the `op` union's `put` leg (no `_iid`, no temporal columns), compose the leg's DUV offsets into `sel` **once per leaf**, and put the put-struct's direct children in `inner`.
  Without it every read goes `sel` → `LegReader.getOffset` → value, two indirections, and `LegReader` lacks `sumInto` (the first smoke run threw `sumInto unsupported on LegReader` in Q1's `SUM`).
  I added `LegReader.sumInto` as well, for the fallback path.
- Ablation toggles: `-Prefine=false` (old evaluate-over-views + compose, still emitting `IndirectRelation`), `-PcomposePutLeg=false`, `-PprojectMode=…`.

## Numbers

SF1, 50 hot reps per JVM, first 20 dropped, interleaved round-robin. All variants use `-PnarrowVarTypes` except `main-plain`.
Every run passed the bench's pinned-result check.
Two sessions on the same box; the box drifted between them (base Q1 ~6.32s in the first, ~5.99s in the second), so compare only within a session.

Session 1 (base / spike / sparse / dense ×3), hot median per JVM, ms:

| variant | Q1 | Q6 | Q1 vs base | Q6 vs base |
|---|---|---|---|---|
| base | 6403 6166 6394 | 4598 4482 4438 | | |
| spike | 6588 6146 6294 | 4663 4349 4388 | +0.3% | −0.9% |
| **sparse** | 6058 5896 5690 | 4180 3885 4017 | **−7.0%** | **−10.6%** |
| dense | 6222 5977 5882 | 4361 4141 4050 | −4.6% | −7.1% |

Session 2 (ablation ×3, plus main without narrowing ×1):

| variant | Q1 | Q6 | Q1 vs base | Q6 vs base |
|---|---|---|---|---|
| base | 5940 6168 5851 | 4124 4176 4222 | | |
| **sparse** | 5298 5352 5306 | 3881 3786 3862 | **−11.1%** | **−7.9%** |
| no refine | 5446 6220 5494 | 4084 4354 4002 | −4.4% (one outlier JVM) | −0.6% |
| no put-leg compose | 5760 5815 5984 | 3823 4000 3786 | −2.2% | −7.3% |
| main-plain (no narrowing) | 6034 | 4168 | ~+1% | ~0% |

- **Q6's win is `refine`**: three predicates over a shrinking array, with no mask vector and no composition, reading direct vectors.
  The put-leg composition adds nothing for Q6.
- **Q1's win needs the put-leg composition**: ~98% of rows go on through the projection and group-by, and every read pays the leg-offset hop without it.
  `refine` alone gives Q1 little; it has one predicate.
- **Sparse beats dense** for both queries.
  Projection is under 3% of samples, so the difference is small; dense even lost on Q1, at 98% rows kept.
- The **spike alone is flat**: composition was only ~2% of scan samples in base.
- Narrowed `var-types` is roughly neutral here (main-plain ≈ base).

runs.py output, session 1:
```
base-1.log           q1   cold   9375 n  30 median    6403 mean    6388 min   5963 sd    228
base-1.log           q6   cold   4932 n  30 median    4598 mean    4627 min   4277 sd    247
base-2.log           q1   cold   9496 n  30 median    6166 mean    6197 min   5751 sd    174
base-2.log           q6   cold   4857 n  30 median    4482 mean    4500 min   4246 sd    150
base-3.log           q1   cold  10523 n  30 median    6394 mean    6446 min   6062 sd    218
base-3.log           q6   cold   4758 n  30 median    4438 mean    4486 min   4120 sd    220
spike-1.log          q1   cold   9629 n  30 median    6588 mean    6636 min   6214 sd    227
spike-1.log          q6   cold   4754 n  30 median    4663 mean    4690 min   4431 sd    157
spike-2.log          q1   cold  10384 n  30 median    6146 mean    6162 min   5882 sd    189
spike-2.log          q6   cold   4947 n  30 median    4349 mean    4353 min   4137 sd    119
spike-3.log          q1   cold  10448 n  30 median    6294 mean    6323 min   6073 sd    199
spike-3.log          q6   cold   4857 n  30 median    4388 mean    4468 min   4279 sd    228
sparse-1.log         q1   cold   9580 n  30 median    6058 mean    6104 min   5559 sd    287
sparse-1.log         q6   cold   4367 n  30 median    4180 mean    4201 min   3935 sd    152
sparse-2.log         q1   cold   9239 n  30 median    5896 mean    5923 min   5581 sd    228
sparse-2.log         q6   cold   4166 n  30 median    3885 mean    3889 min   3675 sd    137
sparse-3.log         q1   cold  10158 n  30 median    5690 mean    5676 min   5353 sd    181
sparse-3.log         q6   cold   4506 n  30 median    4017 mean    4013 min   3703 sd    125
dense-1.log          q1   cold  10027 n  30 median    6222 mean    6266 min   5931 sd    235
dense-1.log          q6   cold   4483 n  30 median    4361 mean    4336 min   4137 sd    113
dense-2.log          q1   cold   9450 n  30 median    5977 mean    6044 min   5741 sd    382
dense-2.log          q6   cold   4455 n  30 median    4141 mean    4131 min   3835 sd    173
dense-3.log          q1   cold   8597 n  30 median    5882 mean    5865 min   5562 sd    166
dense-3.log          q6   cold   4339 n  30 median    4050 mean    4075 min   3865 sd    154
```

runs.py output, session 2:
```
base-1.log           q1   cold   8780 n  30 median    5940 mean    6005 min   5664 sd    271
base-1.log           q6   cold   4453 n  30 median    4124 mean    4129 min   3782 sd    149
base-2.log           q1   cold   8995 n  30 median    6168 mean    6190 min   5883 sd    184
base-2.log           q6   cold   4312 n  30 median    4176 mean    4230 min   3998 sd    190
base-3.log           q1   cold   8945 n  30 median    5851 mean    5868 min   5593 sd    145
base-3.log           q6   cold   4415 n  30 median    4222 mean    4243 min   3983 sd    157
sparse-1.log         q1   cold   7953 n  30 median    5298 mean    5355 min   4986 sd    203
sparse-1.log         q6   cold   4160 n  30 median    3881 mean    3857 min   3584 sd    161
sparse-2.log         q1   cold   7208 n  30 median    5352 mean    5349 min   5021 sd    135
sparse-2.log         q6   cold   3911 n  30 median    3786 mean    3818 min   3606 sd    168
sparse-3.log         q1   cold   8261 n  30 median    5306 mean    5339 min   5110 sd    136
sparse-3.log         q6   cold   4074 n  30 median    3862 mean    3836 min   3619 sd    117
norefine-1.log       q1   cold   8586 n  30 median    5446 mean    5490 min   5169 sd    198
norefine-1.log       q6   cold   4266 n  30 median    4084 mean    4175 min   3963 sd    285
norefine-2.log       q1   cold   9444 n  30 median    6220 mean    6192 min   5740 sd    158
norefine-2.log       q6   cold   4639 n  30 median    4354 mean    4351 min   4142 sd    128
norefine-3.log       q1   cold   8769 n  30 median    5494 mean    5499 min   5282 sd    138
norefine-3.log       q6   cold   4385 n  30 median    4002 mean    4000 min   3811 sd     99
noputleg-1.log       q1   cold   8526 n  30 median    5760 mean    5784 min   5510 sd    168
noputleg-1.log       q6   cold   4101 n  30 median    3823 mean    3848 min   3698 sd     87
noputleg-2.log       q1   cold   9044 n  30 median    5815 mean    5839 min   5470 sd    193
noputleg-2.log       q6   cold   4074 n  30 median    4000 mean    3999 min   3755 sd    110
noputleg-3.log       q1   cold   8923 n  30 median    5984 mean    5988 min   5748 sd    114
noputleg-3.log       q6   cold   4009 n  30 median    3786 mean    3781 min   3644 sd     74
main-plain-1.log     q1   cold   9253 n  30 median    6034 mean    6053 min   5718 sd    145
main-plain-1.log     q6   cold   4341 n  30 median    4168 mean    4187 min   4028 sd     91
```

### JFR breakdown

One run per variant, 12 hot reps, `-PnarrowVarTypes`, Q1 and Q6 mixed. Samples under `ScanCursor.tryAdvance`, from `analyse.py`:

| phase | base | spike | sparse | dense |
|---|---|---|---|---|
| total | 8125 | 8288 | 7478 | 6990 |
| event merge | 2492 | 2385 | 2380 | 2672 |
| group-by | 1779 | 2037 | 2037 | 1528 |
| **predicate eval** | **1765** | 1871 | **1022 (−42%)** | 1243 |
| other | 837 | 708 | 706 | 417 |
| page load | 572 | 596 | 656 | 531 |
| per-batch param-types / emit memo | 525 | 443 | 491 | 484 |
| project | 149 | 242 | 185 | 114 |

- In base, composition (`VectorIndirection.select` ← `IndirectVector.select`, `LegReader.select` ← `RelBuilder.build`, `RelationReader.from`) is ~130 samples (~1.6%).
  That is the ceiling for "compose once" on its own; the win comes from what `refine` removes around the predicate.
- In sparse, the new costs are `RelBuilder.build` (63; the put-leg offset composition) and `VectorIndirection.get` ← `IndirectVector.sumInto` (233).
  The latter is the group-by's `SUM` reading pass-through columns through the view, which base also does via a flattened `IndirectVector`.
- Dense has fewer group-by samples than sparse (1528 vs 2037), yet benches slower.
  My guess is that the summer call sites stay monomorphic in dense, where every column is `IndirectVector(DoubleVector)`, while sparse mixes direct and indirect vectors.
  I haven't verified it.
- The remaining big phases are outside this avenue:
  - event merge, ~31% (`EventRowPointer` comparator, `getValidTo`)
  - group-by key equality: `ByteBuffer.equals`/`BufferMismatch` plus `Buffer.<init>`, ~6%, allocating a `ByteBuffer` per row for Utf8 keys
  - per-batch `->param-types` plus the `lru-memoize` lookup that hashes the expression, ~6.5%

## Data structures

**Matched the problem:**
- One `sel: IntArray` per relation, plus an `inner` relation in a single index space.
  Selecting filters that one array, and every operator that understands the shape reads `inner` at `sel[i]`.
- Folding index spaces once at build time (content `IndirectVector(v, contentSel)` alongside dense temporal columns; DUV leg offsets composed into `sel`).
  This keeps the relation down to one `sel`, which is what lets `refine` filter it rather than compose.
- Codegen keyed on `sel` (`ints`, nil = identity): binding `idx-sym` to `(aget sel i)` was the only change needed in the expression engine.
  The `:variable` codegen already reads `(.setPos rdr idx)`.

**Didn't match:**
- The spike's per-group `sel`: with more than one group a selection still composes once per group, and each `view()` call flattened via `VectorReader.select`.
  For a nested `IndirectVector` or `LegReader`, that composes a fresh array on every `vectorFor`.
- `IndirectVector` over `LegReader`, unflattened: two hops per read, and `LegReader` is missing `sumInto` (added), `squareInto` and `sqrtInto`.
- Mask-then-`IntStream`-then-compose for a selection: three passes and two allocations where one pass and one `int[]` does.

## Suggested sub-goals

Each is a tidy (structure only) or an advance (behaviour); dependencies are in brackets.

1. **tidy**: `IndirectRelation(inner, sel)` as a `RelationReader`.
   Views are unflattened `IndirectVector`; `select` composes once; `openSlice` slices `inner` and keeps `sel`.
   Nothing produces one yet. [—]
2. **tidy**: `SelectionSpec.refine` with its default (select + compose), and `SelectionSpec.selectBy`.
   `ScanCursor`'s column-predicate fold and `SelectCursor` call it, still producing today's relation shape. [1]
3. **advance**: `BitemporalConsumer.build` returns `IndirectRelation`, folding content/temporal index spaces as above. [1, and 6 for aggregations over the temporal fallback]
4. **advance**: the expression engine's `refine` override (generated loop over `sel`).
   This is the Q6 win. [2, 3]
5. **advance**: compose put-leg DUV offsets into `sel` once per leaf when every content column is a put child.
   This is the Q1 win. [3]
   It needs a non-internal way to get a leg's offsets and its inner vector; I added `LegVector.getOffset` as `internal`.
6. **advance**: `LegReader` gets `sumInto`/`squareInto`/`sqrtInto`; alternatively the fallback flattens `IndirectVector(LegReader)` through `LegReader.select` once per leaf.
   This fixes `gcse-statistics`. [3]
7. **advance**: `ProjectionSpec.projectSel` and the expression override, with `ProjectCursor` using it when the input is an `IndirectRelation`.
   Sparse mode only; it's a small win, under 3% of samples. [1]
8. **tidy, optional**: `SelectCursor` passes a non-indirect input through `selectBy` as well, so everything downstream of a select sees one shape. [2]
9. If time: `concatCols` over two `IndirectRelation`s.
   Only possible without composing when both sides share an index space; otherwise fold one side into views.
   I didn't attempt it.

## Questions

- **Q2 (what else iterates `vectors` per batch downstream of the scan?)**, from reading the code:
  - `->expression-projection-spec`'s un-narrowed `var-types`: every column view.
    The narrowing toggle fixes it, and the new `refine`/`projectSel` always narrow.
  - `RelationReader.openSlice` in `ScanCursor`: per batch, every inner column.
    Arrow `transferOwnership`/`associate` is ~1–3% of samples.
  - `coalesce` (`vw/append-rel` → `rowCopier`), only after `SelectCursor`, so not on the Q1/Q6 path.
  - `ApplyCursor` maps `inRel.vectors.map { it.select(sel) }` per column.
  - `RelationReader.rowCopier`, `toArrowStream`, `openArrowRecordBatch`, `schema` (result output).
  - The group-by mapper and `Sum` read only by name.
  - `->param-types` iterates the **args** relation once per spec per batch (~6% of samples together with the emit memo lookup).
    That one is not the input relation, but it is the next per-batch cost.
- Should `SelectCursor` always emit an `IndirectRelation`, as it does now?
  This blocks sub-goal 8, and decides whether the downstream nesting risk below is acceptable.
- Is a public "leg offsets + inner vector" accessor on the DUV leg acceptable, or should `BitemporalConsumer` ask the leg to compose a selection: `leg.composeSel(sel): Pair<VectorReader, IntArray>`?
  Tell-don't-ask favours the latter. This blocks sub-goal 5.
- Dense projection's group-by profile hints at megamorphism in `VectorSummer`.
  Is it worth checking with `-XX:+PrintCompilation`/inlining logs? This blocks nothing.

## Assumptions, risks, broken invariants

- **Fails `xtdb.sql.logic-test.direct-sql-test/gcse-statistics`** (`VAR_POP`/`STDDEV_POP` with `VALID_TIME ALL`).
  The temporal-column fallback reads `IndirectVector(LegReader)`, and `squareInto` is unsupported on `LegReader`.
  I diagnosed this from the code and didn't reproduce it in isolation.
  Test runs: operator and expression namespaces, 383 tests, all green.
  The broader filtered subset (operator/expression/sql/xtql/tpch/indexer/api/query/vector/arrow/logical/temporal/compactor…) ran 1066 tests with this 1 failure.
  The unfiltered `:test` couldn't discover tests in this container, which has no Docker (testcontainers namespaces fail to load); that's environmental.
- **Nested indirection**: `SelectCursor` over a non-indirect input now wraps it as `inner` without flattening.
  Views can therefore be `IndirectVector(IndirectVector(x))`: correct, but two hops, and any `is IndirectVector`/`Slice` special-casing (e.g. `IndirectVector.write` over a selection is unsupported) sees a different shape.
- **`RelationReader` contract**: `vectors`/`vectorForOrNull` return a fresh view per call.
  Identity isn't stable, which the old `FromCols` gave; nothing I found depends on it.
- **Memory**: `IndirectRelation.openSlice` slices the whole `inner` (whole leaf put children) and keeps `sel`.
  That's the same retention as main's `IndirectVector.openSlice`, with no copy.
  Q6 holds 100% of leaf buffers for 1.9% of rows until the batch closes.
  No leaks were reported in the test run.
- `refine` and `projectSel` bypass the input-time `var-types` and always narrow to the expression's variables.
  An expression whose variable is missing from `inner` fails at codegen ("unknown variable") rather than wherever the old path failed.
- The put-leg composition assumes `op` arrives as a `DenseUnionVector` from a loaded page; anything else takes the fallback.
  It also assumes every selected row is a put; the resolver only emits puts, but I didn't verify that against erase/delete-heavy data.
- The JFR runs mix Q1 and Q6, so per-query attribution comes from the ablation numbers, not the profiles.

## Dead-ends

- **The spike as-is**: flat, because composition was ~2% and its group-wise `view()` flattened through `VectorReader.select` (composing an array per view call).
- **Dense projection**: slower than sparse even at 98% kept (Q1), and evaluates 50× more rows than it needs at 1.9% (Q6).
  Projection is too small a share of the time for either choice to matter much.
- **Unflattened views over `LegReader`**: broke `SUM`, and cost an extra hop per read.
  Composing the leg offsets into `sel` once per leaf closed that.

Part of #6285. This is a dart and is not for landing: the branch is evidence for the sub-goals below, and the landed changes get written separately.

Filtering a scan's selection array in place, rather than composing a new one per predicate, makes TPC-H Q6 ~8% faster at SF1; Q1 is flat once the snapshot layout is refreshed.

- **Q6's gain comes from `SelectionSpec.refine`, and it holds in the refreshed layout**
  Q6 hot medians fall 3498 → 3210 ms. Q6 keeps ~1.9% of rows, and its three column predicates each now shrink one `int[]` instead of writing a mask vector, collecting an `IntStream` and composing.

- **Q1's gain disappears in the refreshed layout**
  In the stale-snapshot layout (#6174), with its ~99-row batches, Q1 was 7–11% faster. With ~244-row batches it is +1% (noise). The predicate saving is offset by the group-by, whose key equality is JIT-sensitive (see Group-by keys below).

- **Composition on its own was never the cost**
  It is ~1.6% of scan samples on main. The spike that only composed once per group was flat.

- **Breaks one test: `direct-sql--gcse-statistics` (`xtdb.sql.logic-test.direct-sql-test`)**
  `VAR_POP`/`STDDEV_POP` under `VALID_TIME ALL` hits `squareInto`, which `IndirectVector(LegReader)` doesn't support. The other 1065 tests in the operator/expression/sql/xtql/tpch/indexer subset pass.

Full reports: `dev/perf-6285/REPORT-indirect-select.md` and `dev/perf-6285/REPORT-group-by-keys.md`; raw bench logs are under `dev/perf-6285/results/`.

## What the branch does

- **`IndirectRelation(inner, sel)` holds one selection for the whole relation**
  `select` composes one array however many columns there are. Column views are `IndirectVector(innerVec, sel)`, deliberately not flattened through `VectorReader.select`, which composes an array per column on every call.

- **The scan folds its two index spaces into one when it builds the relation**
  Content columns index leaf rows; temporal columns are dense, one row per emitted event.
  - With no temporal columns, `inner` is the content vectors and `sel` is the content selection.
  - With temporal columns, the content columns become `IndirectVector(v, contentSel)` inside `inner`, and `sel = null`.

- **When every content column is a child of the `op` union's `put` leg, the leg's offsets are composed into `sel` once per leaf**
  `inner` then holds the put struct's own children. Without this, every read goes `sel` → `LegReader.getOffset` → value, and `LegReader` has no `sumInto`, so Q1's `SUM` throws.

- **`SelectionSpec.refine(al, inner, sel, …)` returns the passing subset of `sel`**
  The default implementation selects over the view and composes. The expression engine overrides it with a generated loop that binds `idx` to `sel[i]` and evaluates against `inner` directly; the existing `:variable` codegen needed no change. `ScanCursor`'s column-predicate fold and `SelectCursor` both go through it.

- **`ProjectionSpec.projectSel` evaluates `inner[sel[i]]` into dense output**
  `-Dxtdb.projectMode=dense` instead evaluates every inner row and keeps `sel`. Sparse beat dense on both queries, but projection is under 3% of samples.

- **Each mechanism has a bench toggle**
  `-Prefine=false`, `-PcomposePutLeg=false`, `-PprojectMode=…`, `-PptrEqui`, `-PfastHash`. The commit "dev(bench): hot Q1/Q6 A/B harness" carries the run commands.

## Sub-goals that would land this

Each is a tidy (structure only) or an advance (behaviour); dependencies are in brackets.

- **S1 (tidy): add `IndirectRelation(inner, sel)` with unflattened views; nothing produces one yet** [—]

- **S2 (tidy): add `SelectionSpec.refine`, with its select-then-compose default, and `selectBy`** [S1]
  Wire it into `ScanCursor`'s fold and `SelectCursor`, keeping today's output shape.

- **S3 (advance): `BitemporalConsumer.build` returns an `IndirectRelation`, folding the index spaces** [S1; S6 for aggregates on the temporal path]

- **S4 (advance): the expression engine's generated `refine` loop** [S2, S3]

- **S5 (advance): compose the `put` leg's offsets into `sel` once per leaf** [S3]
  Worth ~3% of Q6 in the refreshed layout. It needs a way to get a leg's offsets and inner vector; see Q3.

- **S6 (advance): `LegReader` implements `squareInto`/`sqrtInto` alongside the `sumInto` added here, or the fallback flattens once per leaf** [S3]
  Fixes `gcse-statistics`.

- **S7 (advance): `ProjectionSpec.projectSel` in sparse mode** [S1]

- **S8 (tidy, optional): `SelectCursor` passes a non-indirect input through `selectBy`** [S2]
  Blocked on Q2.

## Group-by keys

- **Q1's group-by key mapping is ~26% of Q1, ~220 ns/row, to pick one of 4 groups**
  Per row it allocates ~20 short-lived objects, chiefly 2 `MemorySegment`s and a direct `ByteBuffer` per side per key column on every equality check. It also makes ~15–20 megamorphic calls, and on every batch looks up the comparator through `lru-memoize`, which hashes expression maps. Path and per-slice attribution are in `REPORT-group-by-keys.md`.

- **Comparing utf8 keys in place through `ArrowBufPointer` makes Q1 ~4% faster**
  Measured with `-PptrEqui`, on main with the refreshed harness: group-mapping samples −23%, per-batch setup 140 → 25 samples.

- **Unchecked byte reads in the hasher (`-PfastHash`) don't reduce hashing samples**
  The hashing cost is the ~5-hop call chain per key column, not bounds checks.

- **Candidate order: pointer equality in the expression engine's codegen, then resolve the comparator once per cursor, then hash key columns per batch, then a flat open-addressing table**
  Together these plausibly take Q1 10–13% faster without a format change. Dictionary-encoded keys (#4971) would remove nearly all of the 26%, but that is its own project.

## Out of scope

- **A join's output (`concatCols`) over two `IndirectRelation`s wasn't attempted**
  Without composition it only works when both sides share an index space.

## Alternative approaches

- **Per-group selections (the spike "scan emits an IndirectRelation, selection composed once per group") were flat**
  More than one group still composes once per group, and each view flattened through `VectorReader.select`, composing a fresh array on every `vectorFor`.

- **Dense projection was slower than sparse even at 98% of rows kept (Q1)**
  At Q6's 1.9% it evaluates ~50× more rows than it needs.

- **Unflattened views over `LegReader` broke `SUM` and added a hop per read**
  Composing the leg offsets into `sel` once per leaf replaced them.

## Open questions

- **Q1: what else iterates a relation's `vectors` per batch downstream of the scan?**
  Partly answered from the code:
  - The un-narrowed `var-types` in `->expression-projection-spec` touches every column view.
  - `ScanCursor`'s `openSlice` touches every inner column (1–3% in Arrow ownership transfer).
  - The coalescing cursor after `SelectCursor`, `ApplyCursor`'s per-column `select`, and the output path (`rowCopier`, `toArrowStream`, `openArrowRecordBatch`, `schema`) are not on the Q1/Q6 path.
  - `->param-types` iterates the args relation once per spec per batch, ~6% together with the emit memo lookup.

- **Q2: should `SelectCursor` always emit an `IndirectRelation`, as this branch does?**
  It blocks S8, and decides whether the nested-indirection risk below is acceptable.

- **Q3: should the scan read a union leg's offsets directly, or ask the leg for a composed selection, e.g. `leg.composeSel(sel)`?**
  It blocks S5. The branch added an `internal` `LegVector.getOffset`.

- **Q4: is the group-by JIT sensitivity real?**
  `ByteBuffer.equals` took 54 samples in base and 285 on this branch, with the same vector shapes. Pointer equality would remove the question rather than answer it.

## Risks

- **Views can nest**
  `SelectCursor` over a plain relation now wraps it as `inner` without flattening, so views can be `IndirectVector(IndirectVector(x))`. That is correct, but it is two hops per read, and anything special-casing `IndirectVector` (e.g. `write` over a selection) sees a different shape.

- **View identity isn't stable**
  `vectors` and `vectorForOrNull` return a fresh view per call. Nothing found depends on that identity.

- **Q6 retains whole leaf buffers for the batch's lifetime**
  `openSlice` slices all of `inner` and keeps `sel`, so Q6 holds 100% of each leaf's buffers for 1.9% of the rows. That is the same retention as main.

- **`refine` and `projectSel` always narrow `var-types` to the expression's variables**

- **The put-leg composition assumes `op` arrives as a `DenseUnionVector` and every selected row is a put**
  The second wasn't checked against erase- or delete-heavy data.

- **The refreshed-layout numbers drifted across runs**
  Each `-PnoLoad` run adds a refresh transaction to the bench node, and Q1 "off" later ran at 6.4–7.1s against ~5.0s earlier, so only compare within one table.

- **The unfiltered `:test` couldn't discover tests in the dart's container**
  It had no Docker, so testcontainers namespaces fail to load.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01VFCDkmKcVozzPKg5mFZkuQ
